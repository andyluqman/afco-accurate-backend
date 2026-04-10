const uWS = require("uWebSockets.js");
const { callDbProcedure, FbThenPg, initDbFirebird } = require("./dbFb");
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

// Fungsi kirim response JSON selalu OK, untuk error di dalam json
function jsonRes(res, data) {
  if (!res.aborted)
    res.cork(() => {
      res.writeStatus(`200 OK`);
      res.writeHeader("Content-Type", "application/json");
      res.end(JSON.stringify(data));
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
                    `appsaccurate_token=${result[0][1][0]}; HttpOnly; Secure; Path=/; SameSite=Strict`
                  );
                  res.writeHeader("Content-Type", "application/json");
                  res.end(JSON.stringify(["success", result[0][1][1]]));
                });
            })
            .catch((err) => {
              // handle error
              console.error("Error call procedure:", err);
              jsonRes(res, ["__ERROR__", err.message]);
            });
        } catch (e) {
          console.error("Error call procedure:", e);
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
    let buffer = Buffer.alloc(0);

    const proc = req.getParameter(0); // :stored procedure

    const controller = new AbortController();
    const { signal } = controller;


    // Handle abort connection
    res.onAborted(() => {
      res.aborted = true;
      controller.abort();
      console.log("Client aborted connection");
    });

    // Handle incoming data stream
    res.onData((chunk, isLast) => {
      const chunkBuffer = Buffer.from(chunk);
      buffer = Buffer.concat([buffer, chunkBuffer]);
      if (buffer.length > MAX_BODY_SIZE) {
        res.writeStatus("413 Payload Too Large").end();
        res.aborted = true;
        return;
      }
      if (isLast) {
        // Semua data sudah diterima, parse JSON
        if (res.aborted) return; // Kalau sudah abort, skip
        const sessionId = cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1];
        if (!sessionId) {
          jsonRes(
            res,
            ["__ERROR__", "Session expired / tidak ditemukan."],
            res.aborted
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
          jsonRes(res, ["__ERROR__", err.message], res.aborted);
          return;
        }
        const params = body.params || [];
        params.unshift(sessionId);
        callProcedurePgNdjson("accurate.verify_user", params, res, signal);
      }
    });
  })
  .post("/accurate/call/:sp", (res, req) => {
    const cookieHeader = req.getHeader("cookie") || "";
    let buffer = Buffer.alloc(0);

    const proc = req.getParameter(0); // :stored procedure

    const controller = new AbortController();
    const { signal } = controller;


    // Handle abort connection
    res.onAborted(() => {
      res.aborted = true;
      controller.abort();
      console.log("Client aborted connection");
    });

    // Handle incoming data stream
    res.onData((chunk, isLast) => {
      const chunkBuffer = Buffer.from(chunk);
      buffer = Buffer.concat([buffer, chunkBuffer]);
      if (buffer.length > MAX_BODY_SIZE) {
        res.writeStatus("413 Payload Too Large").end();
        res.aborted = true;
        return;
      }
      if (isLast) {
        // Semua data sudah diterima, parse JSON
        if (res.aborted) return; // Kalau sudah abort, skip
        const sessionId = cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1];
        if (!sessionId) {
          jsonRes(
            res,
            ["__ERROR__", "Session expired / tidak ditemukan."],
            res.aborted
          );
          return;
        }
        callProcedurePg("accurate.get_session", [sessionId]).then((result) => { //cek session user untuk mendapatkan info apakah request di encrypt atau tidak, serta dapatkan key untuk decrypt nya
          let raw;
          if (result[0][1][1] === "1")
            raw = decryptData(buffer.toString(), result[0][1][0]);
          else
            raw = buffer.toString();
          let body;
          try {
            body = JSON.parse(raw);
          } catch (err) {
            jsonRes(res, ["__ERROR__", err.message], res.aborted);
            return;
          }
          const params = body.params || [];
          const hasResult = String(body.hasResult ?? "false").toLowerCase() === "true";
          const toPg = body.toPg || "";
          params.unshift(sessionId);
          if (proc === "accurate.verify_actions") {
            // untuk akses ke store procedure firebird
            callProcedurePg(proc, [
              params[0],
              params[1],
              params[2],
              params[3],
              params[4],
            ])
              .then((result) => {
                if (result?.[0]?.[0]?.[0] === "callsp") {
                  if (result?.[0]?.[1]?.[0] === "loadAllDbConfigs") {
                    loadAllDbConfigs()
                      .then((jml) => {
                        console.log(jml + " konfigurasi db fb berhasil dimuat.");
                        jsonRes(
                          res,
                          ["success", jml + " Konfigurasi db fb berhasil dimuat."],
                          res.aborted
                        );
                      })
                      .catch((err) => {
                        console.error("Gagal memuat konfigurasi db fb:", err);
                        jsonRes(res, ["__ERROR__", err.message], res.aborted);
                      });
                  } else if (result?.[0]?.[1]?.[0] === "callDbProcedure") {
                    params.shift(); //hapus param idsession
                    params.shift(); //hapus param path
                    const dbFbName = params.shift(); //ambil dan hapus param dbname
                    const fbSp = params.shift(); //ambil dan hapus param nama store procedure
                    params.push(result[0][1][2]); // insertkan userid accurate
                    callDbProcedure(dbFbName, fbSp, params, hasResult, res);
                  } else if (result?.[0]?.[1]?.[0] === "FbThenPg") {
                    FbThenPg(params).then((result) => {
                      console.log(result, "hasil dari fbthenpg");
                      jsonRes(res, result, res.aborted);
                    })
                      .catch((err) => {
                        // handle error
                        console.error("Error call FbThenPg:", err);
                        jsonRes(res, ["__ERROR__", err.message], res.aborted);
                      });
                  } else if (result?.[0]?.[1]?.[0] === "initDbAccurate") {
                    initDbFirebird(params, res);
                  } else {
                    jsonRes(
                      res,
                      ["__ERROR__", "Procedure " + result?.[0]?.[1]?.[0] + " tidak ditemukan."],
                      res.aborted
                    );
                  }
                } else
                  jsonRes(
                    res,
                    ["__ERROR__", "Output callsp tidak ditemukan."],
                    res.aborted
                  );
              })
              .catch((err) => {
                // handle error
                console.error("Error call procedure:", err);
                jsonRes(res, ["__ERROR__", err.message], res.aborted);
              });
          } else {
            callProcedurePgNdjson(proc, params, res, signal);
          }
        }).catch((err) => {
          // handle error
          console.error("Error call procedure:", err);
          jsonRes(res, ["__ERROR__", err.message], res.aborted);
        });
      }
    });
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