const uWS = require("uWebSockets.js");
const { FbThenPg, initDbFirebird } = require("./dbFb");
const { callDbProcedure } = require("./dbFbAsync");
const { callProcedurePg, callProcedurePgNdjson } = require("./dbPg");
const { loadAllDbConfigs } = require("./configFb");
const fs = require("fs");
const path = require("path");
const isDev = process.env.NODE_ENV !== "production";
const users = new Map();
const socketsByUsername = new Map();
const { activeResponses } = require("./connections");
const MAX_BODY_SIZE = 1 * 1024 * 1024;  // 1MB
const { decryptData } = require("./utils");

function readBody1(res) {
  return new Promise((resolve, reject) => {
    let body = '';

    res.onData((chunk, isLast) => {
      body += Buffer.from(chunk).toString();
      if (isLast) resolve(body);
    });

    res.onAborted(() => reject(new Error('aborted')));
  });
}

function readBody(res, maxSize = 1 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let chunks = [];
    let totalSize = 0;

    res.onData((chunk, isLast) => {
      totalSize += chunk.byteLength;

      if (totalSize > maxSize) {
        reject(new Error('Payload too large'));
        return;
      }

      chunks.push(Buffer.from(chunk));

      if (isLast) {
        resolve(Buffer.concat(chunks).toString());
      }
    });

  });
}

// Fungsi kirim response JSON selalu OK, untuk error di dalam json
function jsonRes(res, data) {
  if (!res.aborted)
    res.cork(() => {
      res.writeStatus(`200 OK`);
      res.writeHeader("Content-Type", "application/json");
      res.write(JSON.stringify(data));
    });
}

function getMimeType(filePath) {
  if (filePath.endsWith(".js")) return "application/javascript";
  if (filePath.endsWith(".css")) return "text/css";
  if (filePath.endsWith(".html")) return "text/html";
  if (filePath.endsWith(".json")) return "application/json";
  if (filePath.endsWith(".png")) return "image/png";
  if (filePath.endsWith(".jpg")) return "image/jpeg";
  if (filePath.endsWith(".svg")) return "image/svg+xml";
  return "application/octet-stream";
}

function serveFile(res, filePath, contentType, encoding) {
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.cork(() => {
        res.writeStatus("404 Not Found").end("Not found");
      });
    } else {
      res.cork(() => {
        res.writeHeader("Content-Type", contentType);
        if (encoding) res.writeHeader("Content-Encoding", encoding);
        res.end(data);
      });
    }
  });
}

function isFile(filePath) {
  try {
    return fs.statSync(filePath).isFile();
  } catch {
    return false;
  }
}

function handleStaticRequest(req, res, baseDir, routePrefix = "") {
  const url = req.getUrl().replace(routePrefix, "") || "/index.html";

  const safePath = path.normalize(url).replace(/^(\.\.(\/|\\|$))+/, "");

  const basePath = path.join(__dirname, baseDir);
  let filePath = path.join(basePath, safePath);

  // Pastikan tidak keluar dari baseDir
  if (!filePath.startsWith(basePath)) {
    res.writeStatus("403 Forbidden").end("Forbidden");
    return;
  }
  fallbackFile = path.join(__dirname, baseDir, "index.html");
  if (!isFile(filePath)) filePath = fallbackFile;
  const ext = path.extname(filePath);
  const mime = getMimeType(filePath);

  if (isDev) {
    // Raw serve in dev
    serveFile(res, filePath, mime);
  } else {
    // Production
    const acceptEncoding = req.getHeader("accept-encoding") || "";

    const brPath = filePath + ".br";
    const gzPath = filePath + ".gz";

    if (acceptEncoding.includes("br") && fs.existsSync(brPath)) {
      serveFile(res, brPath, mime, "br");
    } else if (acceptEncoding.includes("gzip") && fs.existsSync(gzPath)) {
      serveFile(res, gzPath, mime, "gzip");
    } else {
      serveFile(res, filePath, mime);
    }
  }
}

// UWS server
uWS
  .App()
  .post("/accurate/call/login", (res, req) => {
    const cookieHeader = req.getHeader("cookie") || "";
    let body = "";

    // Handle abort connection
    res.onAborted(() => {
      res.aborted = true;
      console.log("Client aborted connection");
    });

    (async () => {
      let responseBody = '';
      let status = '200 OK';

      try {
        const body = await readBody(res);

      } catch (err) {
        responseBody = JSON.stringify({ error: err.message });
      } finally {
        if (!aborted) {
          res.writeStatus(status)
            .writeHeader('Content-Type', 'application/json')
            .end(responseBody);
        }
      }
    })();


    res.onData((chunk, isLast) => {
      body += Buffer.from(chunk).toString();
      if (body.length > MAX_BODY_SIZE) {
        res.writeStatus("413 Payload Too Large").end();
        res.aborted = true;
        return;
      }
      if (isLast) {
        try {
          const sessionId = cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1] || "";
          const now = new Date();
          const year = now.getFullYear();
          const month = String(now.getMonth() + 1).padStart(2, "0");
          const day = String(now.getDate()).padStart(2, "0");
          decryptedBody = decryptData(body, year + day + day + month + year + month);
          const json = JSON.parse(decryptedBody);
          callProcedurePg("accurate.login", [sessionId, json.username, json.password])
            .then((result) => {
              if (!res.aborted)
                res.cork(() => {
                  res.writeHeader(
                    "Set-Cookie",
                    `appsaccurate_token=${result[0][1][0]}; HttpOnly; Path=/; SameSite=Strict`
                  );
                  res.writeHeader("Content-Type", "application/json");
                  res.end(JSON.stringify(["success", result[0][1][1], result[0][1][2]]));
                });
            })
            .catch((err) => {
              // handle error
              console.error("Error call procedure 1:", err);
              jsonRes(res, ["__ERROR__", err.message]);
            });
        } catch (e) {
          console.error("Error call procedure 2:", e);
          jsonRes(res, ["__ERROR__", e.message]);
        }
      }
    });
  })
  .post("/accurate/call/logout", (res, req) => {
    const cookieHeader = req.getHeader("cookie") || "";
    let buffer = Buffer.alloc(0);

    // Handle abort connection
    res.onAborted(() => {
      res.aborted = true;
      console.log("Client aborted connection");
    });

    res.onData((chunk, isLast) => {
      const chunkBuffer = Buffer.from(chunk);
      buffer = Buffer.concat([buffer, chunkBuffer]);
      if (buffer.length > MAX_BODY_SIZE) {
        res.writeStatus("413 Payload Too Large").end();
        res.aborted = true;
        return;
      }
      if (isLast) {
        if (res.aborted) return; // Kalau sudah abort, skip
        const sessionId = cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1];
        if (!sessionId) {
          jsonRes(
            res,
            ["__ERROR__", "Session expired / tidak ditemukan."]
          );
          return;
        }
        const now = new Date();
        const year = now.getFullYear();
        const month = String(now.getMonth() + 1).padStart(2, "0");
        const day = String(now.getDate()).padStart(2, "0");
        decryptedBody = decryptData(buffer.toString(), year + day + day + month + year + month);
        let body;
        try {
          body = JSON.parse(decryptedBody);
        } catch (err) {
          jsonRes(res, ["__ERROR__", err.message]);
          return;
        }
        const params = body.params || [];
        if (!params) {
          jsonRes(res, ["__ERROR__", "Body : params tidak ditemukan."]);
          return;
        }
        params.unshift(sessionId);
        callProcedurePg("accurate.logout", params)
          .then((result) => {
            res.cork(() => {
              res.writeHeader(
                "Set-Cookie",
                `appsaccurate_token=; HttpOnly; Path=/; SameSite=Strict`
              );
              res.writeHeader("Content-Type", "application/json");
              res.end(JSON.stringify(result));
            });
          })
          .catch((err) => {
            // handle error
            console.error("Error call procedure:", err);
            jsonRes(res, ["__ERROR__", err.message]);
          });
      }
    });
  })
  .post("/accurate/call/accurate.verify_user", (res, req) => {
    const cookieHeader = req.getHeader("cookie") || "";

    const controller = new AbortController();
    const { signal } = controller;

    // Handle abort connection
    res.onAborted(() => {
      res.aborted = true;
      controller.abort();
      console.log("Client aborted connection");
    });

    (async () => {
      try {
        const body = await readBody(res);
        const sessionId = cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1];
        if (!sessionId) {
          jsonRes(res, ["__ERROR__", "Session expired / tidak ditemukan."], res.aborted);
          return;
        }
        const now = new Date();
        const year = now.getFullYear();
        const month = String(now.getMonth() + 1).padStart(2, "0");
        const day = String(now.getDate()).padStart(2, "0");
        decryptedBody = decryptData(body, year + day + day + month + year + month);
        let jsonbody = "";
        jsonbody = JSON.parse(decryptedBody);
        const params = jsonbody.params || [];
        params.unshift(sessionId);
        // Header NDJSON harus dikirim segera
        if (!res.aborted)
          res.cork(() => {
            res.writeHeader("Content-Type", "application/x-ndjson");
          });
        await callProcedurePgNdjson("accurate.verify_user", params, res, signal);
      } catch (err) {
        jsonRes(res, ["__ERROR__", err.message], res.aborted);
      } finally {
        if (!res.aborted)
          res.cork(() => {
            res.end();
          });
      }
    })();
  })
  .post("/accurate/call/:sp", (res, req) => {
    const cookieHeader = req.getHeader("cookie") || "";

    const proc = req.getParameter(0); // :stored procedure

    const controller = new AbortController();
    const { signal } = controller;


    // Handle abort connection
    res.onAborted(() => {
      res.aborted = true;
      controller.abort();
      console.log("Client aborted connection");
    });

    (async () => {
      try {
        const buffer = await readBody(res);
        const sessionId = cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1];
        if (!sessionId) {
          if (!res.aborted)
            res.cork(() => {
              res.write(JSON.stringify(["__ERROR__", "Session expired / tidak ditemukan."]));
            });
          return;
        }
        const result = await callProcedurePg("accurate.get_session", [sessionId]);
        let raw;
        if (result[0][1][1] === "1")
          raw = decryptData(buffer, result[0][1][0]);
        else
          raw = buffer;
        const body = JSON.parse(raw);
        const params = body.params || [];
        const hasResult = String(body.hasResult ?? "false").toLowerCase() === "true";
        if (proc === "accurate.verify_actions") {
          pPath = params.shift();
          const is2D = Array.isArray(params[0]) && params.every(p => Array.isArray(p));
          if (is2D) {
            res.cork(() => {
              res.writeHeader("Content-Type", "application/x-ndjson");
            });
            for (let index = 0; index < params.length; index++) {
              await callSp(params[index]);
            }
          } else {
            await callSp(params);
          }

          async function callSp(myParams) {
            const result = await callProcedurePg(proc, [
              sessionId,
              pPath,
              myParams[0],
              myParams[1],
              myParams[2],
            ]);
            if (result?.[0]?.[0]?.[0] === "callsp") {
              if (result?.[0]?.[1]?.[0] === "callDbProcedure") {
                const dbFbName = myParams.shift(); //ambil dan hapus param dbname
                const fbSp = myParams.shift(); //ambil dan hapus param nama store procedure
                myParams.push(result[0][1][2]); // insertkan userid accurate
                await callDbProcedure(dbFbName, fbSp, myParams, hasResult, res);
              } else if (result?.[0]?.[1]?.[0] === "FbThenPg") {
                myParams.push(result[0][1][2]); // insertkan userid accurate
                const resFbThenPg = await FbThenPg(myParams);
                res.cork(() => {
                  res.write(JSON.stringify(resFbThenPg));
                });
              } else {
                if (!res.aborted)
                  res.cork(() => {
                    res.write(JSON.stringify(["__ERROR__", "Procedure " + result?.[0]?.[1]?.[0] + " tidak ditemukan."]));
                  });
              }
            } else
              if (!res.aborted)
                res.cork(() => {
                  res.write(JSON.stringify(["__ERROR__", "Output callsp tidak ditemukan."]));
                });
          }
        } else if (proc === "accurate.verify_actions_confirm") {
          // untuk akses ke store procedure firebird
          const result = await callProcedurePg(proc, [
            params[0],
            params[1],
            params[2],
            params[3],
            params[4],
            params[5],
          ]);
          params.splice(5, 1); //hapus param password confirm
          if (result?.[0]?.[0]?.[0] === "callsp") {
            if (result?.[0]?.[1]?.[0] === "loadAllDbConfigs") {
              loadAllDbConfigs()
                .then((jml) => {
                  console.log(jml + " konfigurasi db fb berhasil dimuat.");
                  if (!res.aborted)
                    res.cork(() => {
                      res.write(JSON.stringify(["success", jml + " Konfigurasi db fb berhasil dimuat."]));
                    });
                })
                .catch((err) => {
                  console.error("Gagal memuat konfigurasi db fb:", err);
                  if (!res.aborted)
                    res.cork(() => {
                      res.write(JSON.stringify(["__ERROR__", err.message]));
                    });
                });
            } else if (result?.[0]?.[1]?.[0] === "callDbProcedure") {
              params.shift(); //hapus param idsession
              params.shift(); //hapus param path
              const dbFbName = params.shift(); //ambil dan hapus param dbname
              const fbSp = params.shift(); //ambil dan hapus param nama store procedure
              params.push(result[0][1][2]); // insertkan userid accurate
              await callDbProcedure(dbFbName, fbSp, params, hasResult, res);
            } else if (result?.[0]?.[1]?.[0] === "FbThenPg") {
              params.push(result[0][1][2]); // insertkan userid accurate
              console.log(result)
              FbThenPg(params).then((result) => {
                console.log(result, "hasil dari fbthenpg");
                if (!res.aborted)
                  res.cork(() => {
                    res.write(JSON.stringify(result));
                  });
              })
                .catch((err) => {
                  // handle error
                  console.error("Error call FbThenPg:", err);
                  if (!res.aborted)
                    res.cork(() => {
                      res.write(JSON.stringify(["__ERROR__", err.message]));
                    });
                });
            } else if (result?.[0]?.[1]?.[0] === "initDbAccurate") {
              initDbFirebird(params, res);
            } else {
              if (!res.aborted)
                res.cork(() => {
                  res.write(JSON.stringify(["__ERROR__", "Procedure " + result?.[0]?.[1]?.[0] + " tidak ditemukan."]));
                });
            }
          } else
            if (!res.aborted)
                res.cork(() => {
                  res.write(JSON.stringify(["__ERROR__", "Output callsp tidak ditemukan."]));
                });
        } else {
          await callProcedurePgNdjson(proc, params, res, signal);
        }
      } catch (err) {
        if (!res.aborted)
          res.cork(() => {
            res.write(JSON.stringify(["__ERROR__", err.message]));
          });
      } finally {
        if (!res.aborted)
          res.cork(() => {
            res.end();
          });
      }
    })();
  })
  .get("/*", (res, req) => {
    res.onAborted(() => {
      console.warn("Request aborted by client");
    });
    handleStaticRequest(req, res, "dist", "/");
  })
  .listen(3030, (token) => {
    if (token) {
      console.log("Server running on port 3030");
      loadAllDbConfigs()
        .then((jml) => {
          console.log(jml + " Konfigurasi db fb berhasil dimuat");
        })
        .catch((err) => {
          console.error("Gagal memuat konfigurasi db fb :", err);
        });
    } else {
      console.log("Failed to start server");
    }
  });

process.on("SIGINT", () => {
  console.log("SIGINT: closing responses");

  for (const res of activeResponses) {
    try {
      res.cork(() => {
        res.write(JSON.stringify(["__ERROR__", "server_shutdown"]) + "\n");
        res.end();
      });
    } catch { }
  }

  process.exit(0);
});