const uWS = require("uWebSockets.js");
const { callFbSp, FbThenPg } = require("./dbFbAsync");
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
//const cronService = require("./cronService");

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
      // 1. Hitung total size terlebih dahulu
      totalSize += chunk.byteLength;

      if (totalSize > maxSize) {
        reject(new Error('Payload too large'));
        return;
      }

      // 2. SOLUSI: Lakukan "Deep Copy" memori chunk agar tidak hilang
      // Kita membuat Buffer baru dan menyalin isi chunk ke dalamnya
      const bufferCopy = Buffer.allocUnsafe(chunk.byteLength);
      Buffer.from(chunk).copy(bufferCopy);

      chunks.push(bufferCopy);

      // 3. Jika sudah chunk terakhir, gabungkan semua
      if (isLast) {
        resolve(Buffer.concat(chunks).toString());
      }
    });

    // Catatan: Di uWS, disarankan juga untuk menghandle res.onAborted, sudah dihandle sebelumnya
    //res.onAborted(() => {
    //  console.log("aborted readbody")
    //  reject(new Error('Request aborted'));
    //});
  });
}

function readBodyOld(res, maxSize = 1 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let chunks = [];
    let totalSize = 0;

    res.onData((chunk, isLast) => {
      console.log(
        "chunk size:",
        chunk.byteLength,
        "isLast:",
        isLast
      );

      totalSize += chunk.byteLength;

      if (totalSize > maxSize) {
        console.log('Payload too large')
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
function jsonRes(res, data, signal) {
  if (!signal.aborted)
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
  let aborted = false;
  res.onAborted(() => {
    aborted = true;
    console.log("Client aborted connection oe");
  });
  fs.readFile(filePath, (err, data) => {
    if (err) {
      if (!aborted)
        res.cork(() => {
          res.writeStatus("404 Not Found").end("Not found");
        });
    } else {
      if (!aborted)
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
    const controller = new AbortController();
    const { signal } = controller;

    // Handle abort connection
    res.onAborted(() => {
      console.log("Client aborted connection > accurate > login");
      controller.abort();
    });

    res.onData((chunk, isLast) => {
      body += Buffer.from(chunk).toString();
      if (body.length > MAX_BODY_SIZE) {
        res.writeStatus("413 Payload Too Large").end();
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
              if (!signal.aborted)
                res.cork(() => {
                  res.writeHeader(
                    "Set-Cookie",
                    `appsaccurate_token=${result[0][1][0]}; HttpOnly; Secure; Path=/; SameSite=Strict`
                  );
                  res.writeHeader("Content-Type", "application/json");
                  res.end(JSON.stringify(["success", result[0][1][1], result[0][1][2]]));
                });
            })
            .catch((err) => {
              // handle error
              console.error("Error call procedure 1:", err);
              jsonRes(res, ["__ERROR__", err.message], signal);
            });
        } catch (e) {
          console.error("Error call procedure 2:", e);
          jsonRes(res, ["__ERROR__", e.message], signal);
        }
      }
    });
  })
  .post("/accurate/call/logout", (res, req) => {
    const cookieHeader = req.getHeader("cookie") || "";
    let buffer = Buffer.alloc(0);

    const controller = new AbortController();
    const { signal } = controller;

    // Handle abort connection
    res.onAborted(() => {
      console.log("Client aborted connection > accurate > logout");
      controller.abort();
    });

    res.onData((chunk, isLast) => {
      const chunkBuffer = Buffer.from(chunk);
      buffer = Buffer.concat([buffer, chunkBuffer]);
      if (buffer.length > MAX_BODY_SIZE) {
        res.writeStatus("413 Payload Too Large").end();
        signal.aborted = true;
        return;
      }
      if (isLast) {
        if (signal.aborted) return; // Kalau sudah abort, skip
        const sessionId = cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1];
        if (!sessionId) {
          jsonRes(
            res,
            ["__ERROR__", "Session expired / tidak ditemukan."],
            signal
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
          jsonRes(res, ["__ERROR__", err.message], signal);
          return;
        }
        const params = body.params || [];
        if (!params) {
          jsonRes(res, ["__ERROR__", "Body : params tidak ditemukan."], signal);
          return;
        }
        params.unshift(sessionId);
        callProcedurePg("accurate.logout", params)
          .then((result) => {
            res.cork(() => {
              res.writeHeader(
                "Set-Cookie",
                `appsaccurate_token=; HttpOnly; Secure; Path=/; SameSite=Strict`
              );
              res.writeHeader("Content-Type", "application/json");
              res.end(JSON.stringify(result));
            });
          })
          .catch((err) => {
            // handle error
            console.error("Error call procedure:", err);
            jsonRes(res, ["__ERROR__", err.message], signal);
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
      console.log("Client aborted connection > accurate > verify_user");
      controller.abort();
    });

    (async () => {
      try {
        if (!signal.aborted)
          res.cork(() => {
            res.writeHeader("Content-Type", "application/x-ndjson");
          });
        const body = await readBody(res);
        const sessionId = cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1];
        if (!sessionId) {
          if (!signal.aborted)
            res.cork(() => {
              res.write(JSON.stringify(["__ERROR__", "Session expired / tidak ditemukan."]));
            });
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
        if (!signal.aborted)
          res.cork(() => {
            res.writeHeader("Content-Type", "application/x-ndjson");
          });
        await callProcedurePgNdjson("accurate.verify_user", params, res, signal);
      } catch (err) {
        if (!signal.aborted)
          res.cork(() => {
            res.write(JSON.stringify(["__ERROR__", err.message]));
          });
      } finally {
        if (!signal.aborted)
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
      console.log("Client aborted connection > accurate call");
      controller.abort();
    });

    (async () => {
      try {
        const buffer = await readBody(res);
        const sessionId = cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1];
        if (!sessionId) {
          if (!signal.aborted)
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
        if (proc === "accurate.verify_actions" || proc === "accurate.verify_actions_confirm") { //execute procedur di firebird tapi di verifikasi dahulu di postgres
          const pPath = params.shift();
          await callFbSp(proc, sessionId, pPath, params, hasResult, res, signal);
        }/* else if (proc === "accurate.fbthenpg") {
          params.push(result[0][1][2]); // insertkan userid accurate
          const resFbThenPg = await FbThenPg(sessionId, params, signal);
          if (!signal.aborted)
            res.cork(() => {
              res.write(JSON.stringify(resFbThenPg));
            });
        }*/ else {
          params.unshift(sessionId);
          await callProcedurePgNdjson(proc, params, res, signal);
        }
      } catch (err) {
        if (!signal.aborted)
          res.cork(() => {
            res.write(JSON.stringify(["__ERROR__", err.message]));
          });
      } finally {
        //console.log(signal.aborted);
        if (!signal.aborted) {
          res.cork(() => {
            res.end();
          });
        }
      }
    })();
  })
  .get("/*", (res, req) => {
    res.onAborted(() => {
      console.warn("Request aborted by client");
    });
    handleStaticRequest(req, res, "dist", "/accurate/");
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
