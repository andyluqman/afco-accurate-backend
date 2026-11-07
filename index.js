const uWS = require("uWebSockets.js");
const { callFbSp, FbThenPg } = require("./dbFbAsync");
const { callProcedurePg, callProcedurePgNdjson } = require("./dbPg");
const { loadAllDbConfigs } = require("./configFb");
const fs = require("fs");
const path = require("path");
const isDev = process.env.NODE_ENV !== "production";
const clients = new Map();
const wsToUser = new Map();
const { activeResponses } = require("./connections");
const { listen: listenPgNotify } = require("./pgListener");
const { getTokensForUser, sendNotification } = require("./fcmService");
const { sendWebhook } = require("./webhookService");
const MAX_BODY_SIZE = 1 * 1024 * 1024;  // 1MB
const PORT = Number(process.env.PORT || 3030);
const { decryptData } = require("./utils");
const Busboy = require("busboy");
const storage = require("./storageService");
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

function broadcast(data) {
  console.log(data,"bcast");
  const msg = JSON.stringify(data);
  clients.forEach((ws) => {
    ws.send(msg);
  });
}

// Resolusi nama file -> object yang benar-benar ada di storage.
// Name file yang diminta bisa berupa nama persis (dari kolom nmfile)
// atau hanya nama dasar/rowId (mis. "2050.jpg"), lalu dicari file terbaru
// dengan prefix tersebut.
async function resolveUploadObject(filename, folder = "") {
  let decodedName = filename;
  try {
    decodedName = decodeURIComponent(filename);
  } catch {
    decodedName = filename;
  }

  const normFolder = storage.normalizeFolder ? storage.normalizeFolder(folder) : "";
  let target = path.basename(decodedName);
  const stat = await storage.statFile(target, normFolder);
  if (stat) {
    return { objectName: target, stat, requested: decodedName, folder: normFolder };
  }

  const baseName = path.basename(decodedName, path.extname(decodedName));
  const safeBase = baseName.replace(/[^a-zA-Z0-9_\-]/g, "_");
  let latestFile = await storage.findLatestFile(safeBase, normFolder);
  if (!latestFile && safeBase !== baseName) {
    latestFile = await storage.findLatestFile(baseName, normFolder);
  }
  if (!latestFile) return null;

  return { objectName: latestFile, stat: null, requested: decodedName, folder: normFolder };
}

// Helper: parse nmfile yang tersimpan di DB.
  // nmfile bisa berupa:
// - "pindah-barang/4063_123.jpg" (relative path, sudah termasuk folder)
// - "4063_123.jpg"                (hanya filename, folder diambil dari parameter/request)

function parseStoredPath(nmfile, fallbackFolder = "") {
  if (!nmfile) return { objectName: "", folder: storage.normalizeFolder(fallbackFolder) };
  const clean = String(nmfile).replace(/\\/g, "/").replace(/^\/+/, "");
  const parts = clean.split("/").filter(Boolean);
  if (parts.length <= 1) {
    return {
      objectName: path.basename(clean),
      folder: storage.normalizeFolder(fallbackFolder),
    };
  }
  // Last segment is filename, rest may include a folder like "foto"
  const objectName = parts.pop();
  const folderFromPath = parts.join("/");
  return {
    objectName,
    folder: storage.normalizeFolder(folderFromPath) || storage.normalizeFolder(fallbackFolder),
  };
}
const MAX_PRESIGN_EXPIRY = 7 * 24 * 60 * 60;

function parseExpiry(raw, fallback = 3600) {
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) return fallback;
  return Math.min(Math.floor(value), MAX_PRESIGN_EXPIRY);
}

// Ambil session id dari cookie; null kalau tidak ada / tidak valid.
function getSessionId(req) {
  const cookieHeader = req.getHeader("cookie") || "";
  return cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1] || null;
}

// Validasi nama file: cegah path traversal.
function isSafeFileName(filename) {
  return Boolean(
    filename &&
    !filename.includes("..") &&
    !filename.includes("/") &&
    !filename.includes("\\")
  );
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
          callProcedurePg("accurate.login", [sessionId, json.username, json.password, json.deviceId])
            .then((result) => {
              if (!signal.aborted)
                res.cork(() => {
                  res.writeHeader(
                    "Set-Cookie",
                    `appsaccurate_token=${result[0][1][0]}; HttpOnly; Secure; Path=/; SameSite=Strict`
                  );
                  res.writeHeader("Content-Type", "application/json");
                  res.end(JSON.stringify(["success", result[0][1][1], result[0][1][2], result[0][1][3]]));
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
        const cekEdit = body.cekEdit !== undefined ? String(body.cekEdit).toLowerCase() === "true" : true;
        if (proc === "accurate.verify_actions" || proc === "accurate.verify_actions_confirm") { //execute procedur di firebird tapi di verifikasi dahulu di postgres
          const pPath = params.shift();
          await callFbSp(proc, sessionId, pPath, params, hasResult, res, signal, cekEdit);
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
  .post("/accurate/upload-bulk", (res, req) => {
    const cookieHeader = req.getHeader("cookie") || "";
    const sessionId = cookieHeader.match(/appsaccurate_token=([^;]+)/)?.[1];
    const contentType = req.getHeader("content-type") || "";

    if (!sessionId) {
      res.cork(() => {
        res.writeStatus("401 Unauthorized");
        res.writeHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ success: false, message: "Session tidak ditemukan." }));
      });
      return;
    }

    if (!contentType.includes("multipart/form-data")) {
      res.cork(() => {
        res.writeStatus("400 Bad Request");
        res.writeHeader("Content-Type", "application/json");
        res.end(JSON.stringify({ success: false, message: "Content-Type harus multipart/form-data." }));
      });
      return;
    }

    const uploadedFiles = [];
    let chunks = [];
    let totalSize = 0;
    const MAX_UPLOAD_SIZE = 50 * 1024 * 1024;

    const controller = new AbortController();
    const { signal } = controller;

    res.onAborted(() => {
      console.log("Koneksi upload-bulk dibatalkan oleh client");
      controller.abort();
    });

    res.onData((chunk, isLast) => {
      if (signal.aborted) return;

      totalSize += chunk.byteLength;

      if (totalSize > MAX_UPLOAD_SIZE) {
        res.cork(() => {
          res.writeStatus("413 Payload Too Large");
          res.writeHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ success: false, message: "Ukuran upload melebihi batas maksimal (50MB)." }));
        });
        controller.abort();
        return;
      }

      const bufferCopy = Buffer.allocUnsafe(chunk.byteLength);
      Buffer.from(chunk).copy(bufferCopy);
      chunks.push(bufferCopy);

      if (isLast) {
        (async () => {
          try {
            const sessionResult = await callProcedurePg("accurate.get_session", [sessionId]);
            if (!sessionResult) {
              if (!signal.aborted) {
                res.cork(() => {
                  res.writeStatus("401 Unauthorized");
                  res.writeHeader("Content-Type", "application/json");
                  res.end(JSON.stringify({ success: false, message: "Session tidak valid." }));
                });
              }
              return;
            }

              const busboy = Busboy({ headers: { "content-type": contentType } });
              const fullBuffer = Buffer.concat(chunks);
              const allowedTypes = ["image/jpeg", "image/png", "image/gif", "image/webp"];
              const MAX_FILES = 20;
              let fileCount = 0;
              let busboyFinished = false;
              let pendingUploads = 0;
              const uploadErrors = [];
              let uploadFolder = "";

              busboy.on("field", (fieldname, val) => {
                if (fieldname === "folder" || fieldname === "path" || fieldname === "type" || fieldname === "module" || fieldname === "dir") {
                  const norm = storage.normalizeFolder ? storage.normalizeFolder(val) : "";
                  if (norm) uploadFolder = norm;
                }
              });

              busboy.on("file", (fieldname, fileStream, info) => {
                const { filename, mimeType } = info;
                console.log(`Menerima file: ${filename} (${mimeType})`);

                fileCount++;
                if (fileCount > MAX_FILES) {
                  fileStream.resume();
                  return;
                }

                if (!allowedTypes.includes(mimeType)) {
                  fileStream.resume();
                  uploadErrors.push(`Tipe file tidak diizinkan: ${filename} (${mimeType})`);
                  return;
                }

                const ext = path.extname(filename).toLowerCase();
                const baseName = path.basename(filename, ext);
                const safeBase = baseName.replace(/[^a-zA-Z0-9_\-]/g, "_");
                const timestamp = Date.now();
                const newFilename = `${safeBase}_${timestamp}${ext}`;

                const parts = [];
                pendingUploads++;
                fileStream.on("data", (data) => parts.push(data));
                fileStream.on("error", (err) => {
                  pendingUploads--;
                  uploadErrors.push(`Gagal membaca file ${filename}: ${err.message}`);
                });
                fileStream.on("end", async () => {
                  try {
                    const buffer = Buffer.concat(parts);
                    const saved = await storage.putFile(newFilename, buffer, mimeType, uploadFolder);
                    uploadedFiles.push({
                      filename: newFilename,
                      relative: saved.relative,
                      folder: saved.folder || "",
                      idest: safeBase,
                      size: saved.size,
                      storage: saved.storage
                    });
                    console.log(`File ${newFilename} disimpan ke ${saved.storage}${saved.folder ? `/${saved.folder}` : ""}`);
                  } catch (err) {
                    console.error("Gagal menyimpan file:", err);
                    uploadErrors.push(`Gagal menyimpan file ${newFilename}: ${err.message}`);
                  } finally {
                    pendingUploads--;
                    finishUpload();
                  }
                });
              });

              const finishUpload = async () => {
                if (!busboyFinished || pendingUploads !== 0 || signal.aborted) return;

                // Simpan ke PostgreSQL via stored procedure (bulk)
                if (uploadedFiles.length > 0) {
                  try {
                    // Kirim path relatif MinIO (mis. foto/123_456.jpg atau 123_456.jpg) ke nmfile
                    const filenames = uploadedFiles.map(f => f.relative || f.filename);
                    const idests = uploadedFiles.map(f => f.idest);
                    await callProcedurePg("accurate.upload_bulk_save", [sessionId, filenames, idests]);
                    console.log(`${uploadedFiles.length} file records saved to PostgreSQL`);
                  } catch (dbErr) {
                    console.error("Gagal menyimpan ke PostgreSQL:", dbErr);
                  }
                }
                if (uploadedFiles.length === 0 && uploadErrors.length > 0) {
                  res.cork(() => {
                    res.writeStatus("500 Internal Server Error");
                    res.writeHeader("Content-Type", "application/json");
                    res.end(JSON.stringify({
                      success: false,
                      message: uploadErrors[0],
                      errors: uploadErrors
                    }));
                  });
                  return;
                }
                res.cork(() => {
                  res.writeHeader("Content-Type", "application/json");
                  res.end(JSON.stringify({
                    success: true,
                    message: `${uploadedFiles.length} file berhasil diupload.`,
                    storage: storage.isMinioEnabled ? "minio" : "local",
                    files: uploadedFiles,
                    errors: uploadErrors
                  }));
                });
              };

              busboy.on("finish", () => {
                busboyFinished = true;
                finishUpload();
              });

              busboy.on("error", (err) => {
                console.error("Busboy error:", err);
                if (!signal.aborted) {
                  res.cork(() => {
                    res.writeStatus("500 Internal Server Error");
                    res.writeHeader("Content-Type", "application/json");
                    res.end(JSON.stringify({ success: false, message: "Gagal memproses file." }));
                  });
                }
              });

              busboy.write(fullBuffer);
              busboy.end();
          } catch (err) {
            console.error("Gagal memproses bulk upload:", err);
            if (!signal.aborted) {
              res.cork(() => {
                res.writeStatus("500 Internal Server Error");
                res.writeHeader("Content-Type", "application/json");
                res.end(JSON.stringify({ success: false, message: "Gagal memproses file." }));
              });
            }
          }
        })();
      }
    });
  })
  .get("/accurate/uploads/:folder/:filename", (res, req) => {
    const sessionId = getSessionId(req);
    const folder = req.getParameter(0);
    const filename = req.getParameter(1);
    let aborted = false;
    const wantRedirect = req.getQuery("redirect");
    const presignedExpiry = parseExpiry(req.getQuery("expires"));

    res.onAborted(() => {
      aborted = true;
      console.log("Client aborted upload file request");
    });

    if (!sessionId) {
      res.cork(() => {
        res.writeStatus("401 Unauthorized").end("Session tidak ditemukan.");
      });
      return;
    }

    if (!isSafeFileName(filename)) {
      res.cork(() => {
        res.writeStatus("400 Bad Request").end("Nama file tidak valid.");
      });
      return;
    }

    const normFolder = storage.normalizeFolder ? storage.normalizeFolder(folder) : "";
    if (!normFolder) {
      res.cork(() => {
        res.writeStatus("400 Bad Request").end("Folder tidak valid.");
      });
      return;
    }

    const mimeMap = {
      ".jpg": "image/jpeg",
      ".jpeg": "image/jpeg",
      ".png": "image/png",
      ".gif": "image/gif",
      ".webp": "image/webp",
      ".svg": "image/svg+xml",
      ".pdf": "application/pdf"
    };

    (async () => {
      try {
        const resolved = await resolveUploadObject(filename, normFolder);
        if (!resolved) {
          res.cork(() => {
            res.writeStatus("404 Not Found").end("File tidak ditemukan.");
          });
          return;
        }
        const target = resolved.objectName;

        if (wantRedirect === "1" || wantRedirect === "true") {
          const presigned = await storage.getPresignedUrl(target, presignedExpiry, normFolder);
          if (presigned) {
            res.cork(() => {
              res.writeStatus("302 Found");
              res.writeHeader("Location", presigned);
              res.writeHeader("Cache-Control", "no-store");
              res.end();
            });
            return;
          }
        }

        const contentType =
          mimeMap[path.extname(target).toLowerCase()] || "application/octet-stream";
        const stream = await storage.getFileStream(target, normFolder);

        res.cork(() => {
          res.writeHeader("Content-Type", contentType);
          res.writeHeader("Cache-Control", "no-cache");
        });

        stream.on("data", (chunk) => {
          if (!aborted)
            res.cork(() => {
              res.write(chunk);
            });
        });
        stream.on("end", () => {
          if (!aborted)
            res.cork(() => {
              res.end();
            });
        });
        stream.on("error", (err) => {
          console.error("Gagal mengirim file:", err);
          if (!aborted)
            res.cork(() => {
              res.end();
            });
        });
      } catch (err) {
        console.error("Gagal membaca file:", err);
        res.cork(() => {
          res.writeStatus("500 Internal Server Error").end("Gagal membaca file.");
        });
      }
    })();
  })
  .get("/accurate/uploads/:filename/url", (res, req) => {
    const sessionId = getSessionId(req);
    const filename = req.getParameter(0);
    // req tidak boleh diakses setelah await -> ambil query sekarang
    const expiresIn = parseExpiry(req.getQuery("expires"));
    const folderParam = req.getQuery("folder");

    res.onAborted(() => {
      console.log("Client aborted presigned url request");
    });

    if (!sessionId) {
      res.cork(() => {
        res.writeStatus("401 Unauthorized").end("Session tidak ditemukan.");
      });
      return;
    }

    if (!isSafeFileName(filename)) {
      res.cork(() => {
        res.writeStatus("400 Bad Request").end("Nama file tidak valid.");
      });
      return;
    }

    const normFolder = storage.normalizeFolder ? storage.normalizeFolder(folderParam) : "";

    (async () => {
      try {
        if (!storage.isMinioEnabled) {
          res.cork(() => {
            res.writeStatus("501 Not Implemented");
            res.writeHeader("Content-Type", "application/json");
            res.end(JSON.stringify({
              success: false,
              message: "MinIO tidak dikonfigurasi, gunakan /accurate/uploads/:filename."
            }));
          });
          return;
        }

        const resolved = await resolveUploadObject(filename, normFolder);
        if (!resolved) {
          res.cork(() => {
            res.writeStatus("404 Not Found").end("File tidak ditemukan.");
          });
          return;
        }

        const url = await storage.getPresignedUrl(resolved.objectName, expiresIn, normFolder);

        res.cork(() => {
          res.writeHeader("Content-Type", "application/json");
          res.writeHeader("Cache-Control", "no-store");
          res.end(JSON.stringify({
            success: true,
            url,
            filename: resolved.objectName,
            relative: normFolder ? `${normFolder}/${resolved.objectName}` : resolved.objectName,
            folder: normFolder,
            expiresIn,
            storage: "minio"
          }));
        });
      } catch (err) {
        console.error("Gagal membuat presigned URL:", err);
        res.cork(() => {
          res.writeStatus("500 Internal Server Error").end("Gagal membuat presigned URL.");
        });
      }
    })();
  })
  .post("/accurate/list_img", (res, req) => {
    const sessionId = getSessionId(req);
    let aborted = false;
    res.onAborted(() => {
      aborted = true;
      console.log("Client aborted foto list request");
    });

    (async () => {
      try {
        if (!sessionId) {
          if (!aborted) res.cork(() => {
            res.writeStatus("401 Unauthorized");
            res.writeHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ success: false, message: "Session tidak ditemukan." }));
          });
          return;
        }

        const raw = await readBody(res, 256 * 1024);
        let body = {};
        try { body = JSON.parse(raw || "{}"); } catch { body = {}; }

        const idest = body.idest ?? body.id;
        if (idest === undefined || idest === null || String(idest).trim() === "") {
          if (!aborted) res.cork(() => {
            res.writeStatus("400 Bad Request");
            res.writeHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ success: false, message: "Parameter idest wajib diisi." }));
          });
          return;
        }

        const folderHint = storage.normalizeFolder ? storage.normalizeFolder(body.folder) : "";
        const schemaRaw = String(body.schema || "").replace(/[^a-zA-Z0-9_\- ]/g, "");
        const schema = schemaRaw ? `"${schemaRaw}"` : "";
        const table = schema ? `${schema}.pindah_barang_foto` : "pindah_barang_foto";

        // Ambil daftar foto dari DB
        let rows = [];
        try {
          const result = await callProcedurePg("accurate.get_img_by_id", [sessionId, idest, folderHint || ""]);
          // callProcedurePg mengembalikan array 2D: hasil = [ [header, ...rows], ... ] atau [ [header,outvar], [cursorRows...] ]
          // Coba ambil dari cursor result set pertama yang berbentuk array
          for (const set of result) {
            if (Array.isArray(set) && set.length > 1 && Array.isArray(set[0]) && set[0].includes("nmfile")) {
              const header = set[0];
              const nmIdx = header.indexOf("nmfile");
              const idIdx = header.indexOf("id") >= 0 ? header.indexOf("id") : header.indexOf("idest");
              rows = set.slice(1).map(r => ({ nmfile: r[nmIdx], idest: idIdx >= 0 ? r[idIdx] : idest }));
              break;
            }
          }
          // Fallback: jika format berbeda, coba langsung parse
          if (rows.length === 0 && result.length > 0) {
            for (const set of result) {
              if (Array.isArray(set)) {
                for (const row of set) {
                  if (Array.isArray(row) && typeof row[0] === "string" && (row[0].includes("/") || row[0].match(/\.(jpg|jpeg|png|gif|webp)$/i))) {
                    rows.push({ nmfile: row[0], idest: row[1] ?? idest });
                  }
                }
              }
            }
          }
        } catch (spErr) {
          // Fallback: query langsung jika SP belum tersedia
          console.log("SP accurate.get_foto_by_idest gagal, fallback query langsung:", spErr.message);
          const { pgPool } = require("./configPg");
          const client = await pgPool.connect();
          try {
            const q = `SELECT nmfile, idest FROM ${table} WHERE idest = $1 ORDER BY 1`;
            const qr = await client.query(q, [String(idest)]);
            rows = qr.rows;
          } finally {
            client.release();
          }
        }

        // Bangun response dengan URL presigned (atau URL proxy)
        const files = [];
        for (const row of rows) {
          const nmfile = row.nmfile;
          if (!nmfile) continue;
          const parsed = parseStoredPath(nmfile, folderHint);
          const objName = parsed.objectName;
          const folder = parsed.folder;
          let url = null;
          if (storage.isMinioEnabled) {
            url = await storage.getPresignedUrl(objName, 3600, folder);
          }
          // URL proxy selalu tersedia sebagai fallback
          const proxyUrl = folder
            ? `/accurate/uploads/${encodeURIComponent(folder)}/${encodeURIComponent(objName)}`
            : `/accurate/uploads/${encodeURIComponent(objName)}`;
          files.push({
            nmfile,
            filename: objName,
            folder,
            idest: row.idest,
            url: url || proxyUrl,
            proxyUrl,
            direct: !!url,
          });
        }
        if (!aborted) res.cork(() => {
          res.writeHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ success: true, count: files.length, files }));
        });
      } catch (err) {
        console.error("Gagal ambil daftar foto:", err);
        if (!aborted) res.cork(() => {
          res.writeStatus("500 Internal Server Error");
          res.writeHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ success: false, message: err.message }));
        });
      }
    })();
  })
  .get("/accurate/foto/by-id", (res, req) => {
    const sessionId = getSessionId(req);
    let aborted = false;
    // req tidak boleh diakses setelah await -> ambil query sekarang
    const idest = req.getQuery("idest") || req.getQuery("id");
    const folderParam = req.getQuery("folder");
    const schemaParam = req.getQuery("schema");
    const mode = req.getQuery("mode"); // "redirect" = 302 ke foto pertama; default = JSON list
    const indexParam = req.getQuery("index");

    res.onAborted(() => {
      aborted = true;
      console.log("Client aborted foto by-id request");
    });

    (async () => {
      try {
        if (!sessionId) {
          if (!aborted) res.cork(() => {
            res.writeStatus("401 Unauthorized");
            res.writeHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ success: false, message: "Session tidak ditemukan." }));
          });
          return;
        }

        if (idest === undefined || idest === null || String(idest).trim() === "") {
          if (!aborted) res.cork(() => {
            res.writeStatus("400 Bad Request");
            res.writeHeader("Content-Type", "application/json");
            res.end(JSON.stringify({ success: false, message: "Parameter idest wajib diisi." }));
          });
          return;
        }

        const folderHint = storage.normalizeFolder ? storage.normalizeFolder(folderParam) : "";
        const schemaRaw = String(schemaParam || "").replace(/[^a-zA-Z0-9_\- ]/g, "");
        const schema = schemaRaw ? `"${schemaRaw}"` : "";
        const table = schema ? `${schema}.pindah_barang_foto` : "pindah_barang_foto";

        // Ambil daftar foto dari DB
        let rows = [];
        try {
          const result = await callProcedurePg("accurate.get_img_by_id", [sessionId, String(idest), folderHint || ""]);
          for (const set of result) {
            if (Array.isArray(set) && set.length > 1 && Array.isArray(set[0]) && set[0].includes("nmfile")) {
              const header = set[0];
              const nmIdx = header.indexOf("nmfile");
              const idIdx = header.indexOf("id") >= 0 ? header.indexOf("id") : header.indexOf("idest");
              rows = set.slice(1).map(r => ({ nmfile: r[nmIdx], idest: idIdx >= 0 ? r[idIdx] : idest }));
              break;
            }
          }
          if (rows.length === 0 && result.length > 0) {
            for (const set of result) {
              if (Array.isArray(set)) {
                for (const row of set) {
                  if (Array.isArray(row) && typeof row[0] === "string" && (row[0].includes("/") || row[0].match(/\.(jpg|jpeg|png|gif|webp)$/i))) {
                    rows.push({ nmfile: row[0], idest: row[1] ?? idest });
                  }
                }
              }
            }
          }
        } catch (spErr) {
          console.log("SP accurate.get_img_by_id gagal, fallback query langsung:", spErr.message);
          const { pgPool } = require("./configPg");
          const client = await pgPool.connect();
          try {
            const q = `SELECT nmfile, idest FROM ${table} WHERE idest = $1 ORDER BY 1`;
            const qr = await client.query(q, [String(idest)]);
            rows = qr.rows;
          } finally {
            client.release();
          }
        }

        // Bangun response dengan URL presigned/proxy
        const files = [];
        for (const row of rows) {
          const nmfile = row.nmfile;
          if (!nmfile) continue;
          const parsed = parseStoredPath(nmfile, folderHint);
          const objName = parsed.objectName;
          const folder = parsed.folder;
          let url = null;
          if (storage.isMinioEnabled) {
            url = await storage.getPresignedUrl(objName, 3600, folder);
          }
          const proxyUrl = folder
            ? `/accurate/uploads/${encodeURIComponent(folder)}/${encodeURIComponent(objName)}`
            : `/accurate/uploads/${encodeURIComponent(objName)}`;
          files.push({
            nmfile,
            filename: objName,
            folder,
            idest: row.idest,
            url: url || proxyUrl,
            proxyUrl,
            direct: !!url,
          });
        }

        // mode=redirect: langsung 302 ke foto (index, default 0)
        if (mode === "redirect" || mode === "302") {
          const idx = Math.max(0, parseInt(indexParam, 10) || 0);
          const target = files[idx];
          if (!target) {
            if (!aborted) res.cork(() => {
              res.writeStatus("404 Not Found").end("File tidak ditemukan.");
            });
            return;
          }
          if (!aborted) res.cork(() => {
            res.writeStatus("302 Found");
            res.writeHeader("Location", target.url);
            res.writeHeader("Cache-Control", "no-store");
            res.end();
          });
          return;
        }

        if (!aborted) res.cork(() => {
          res.writeHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ success: true, count: files.length, files }));
        });
      } catch (err) {
        console.error("Gagal ambil daftar foto:", err);
        if (!aborted) res.cork(() => {
          res.writeStatus("500 Internal Server Error");
          res.writeHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ success: false, message: err.message }));
        });
      }
    })();
  })
  .get("/*", (res, req) => {
    res.onAborted(() => {
      console.warn("Request aborted by client");
    });
    handleStaticRequest(req, res, "dist", "/accurate/");
  })
  .ws("/accurate/ws", {
    message: (ws, message) => {
      try {
        const text = Buffer.from(message).toString("utf8");
        const data = JSON.parse(text);
        if (data.type !== "presence") return;

        if (data.action === "join") {
          const username = data.username;
          if (typeof username !== "string" || !username.trim()) return;

          const trimmed = username.trim();

          if (clients.has(trimmed)) {
            const oldWs = clients.get(trimmed);
            wsToUser.delete(oldWs);
          }

          const prevUsername = wsToUser.get(ws);
          if (prevUsername) {
            clients.delete(prevUsername);
          }

          clients.set(trimmed, ws);
          wsToUser.set(ws, trimmed);

          ws.send(JSON.stringify({ type: "presence", action: "init", users: Array.from(clients.keys()) }));
          broadcast({ type: "presence", action: "joined", username: trimmed });
        } else if (data.action === "leave") {
          const username = data.username;
          if (typeof username !== "string" || !username.trim()) return;

          const trimmed = username.trim();
          const actualUsername = wsToUser.get(ws);
          if (actualUsername !== trimmed) return;

          clients.delete(trimmed);
          wsToUser.delete(ws);
          broadcast({ type: "presence", action: "left", username: trimmed });
        }
      } catch (e) {
        console.log("Error websocket on message : ", e);
      }
    },
    close: (ws) => {
      const username = wsToUser.get(ws);
      if (username) {
        clients.delete(username);
        wsToUser.delete(ws);
        broadcast({ type: "presence", action: "left", username });
      }
    }
  })
  .listen(PORT, (token) => {
    if (token) {
      console.log("Server running on port " + PORT);
      loadAllDbConfigs()
        .then((jml) => {
          console.log(jml + " Konfigurasi db fb berhasil dimuat");
        })
        .catch((err) => {
          console.error("Gagal memuat konfigurasi db fb :", err);
        });

      // Mulai mendengarkan notifikasi PostgreSQL (LISTEN/NOTIFY)
      listenPgNotify("pg_notify", async (payload) => {
        console.log("NOTIFY :", payload);

        const userId = payload && payload.jenis;
        if (!userId) {
          console.warn("pg_notify: payload tidak punya jenis pesan.");
          return;
        }

        if (payload.jenis === 'fcm') {
          try {
            //const tokens = await getTokensForUser(userId);
            //if (tokens.length === 0) {
            //  console.log(`new_realisasi: tidak ada FCM token untuk user ${userId}`);
            //  return;
            //}
            const tokens = ["cxAiKfgrpeTvtjGcGb3m2Z:APA91bHxC9KQDelDC5--XfBM-bFCj1PZPtO24dutM1i1vp-pjFWpcs9eAxqTql3KWSYopuPRrrciS93BrTvj6n3BS0wQKkagMJlTWhQmUc9nevK3LobBapQ"]
            const orderId = payload.order_id;
            const result = await sendNotification({
              tokens,
              title: "Pesanan Baru",
              body: `Ada pesanan baru${orderId ? ` #${orderId}` : ""}`,
              data: {
                user_id: String(userId),
                order_id: orderId != null ? String(orderId) : "",
                type: "new_order",
              },
            });
            console.log("FCM terkirim:", result);

            // Kirim juga ke webhook (mis. WhatsApp gateway)
            const whResult = await sendWebhook({
              clientId: "1",
              number: "6289666293998",
              text: "Halo dari Node.js"
            });
            console.log("Webhook terkirim:", whResult.status, whResult.body);
          } catch (err) {
            console.error("Gagal mengirim notifikasi untuk new_realisasi:", err);
          }
        } else if (payload.jenis === 'whatapps') {
          try {
            // Kirim juga ke webhook (mis. WhatsApp gateway)
            const whResult = await sendWebhook({
              clientId: payload.clientId,
              number: payload.number,
              text: payload.text
            });
            console.log("Webhook terkirim:", whResult.status, whResult.body);
          } catch (err) {
            console.error("Gagal mengirim notifikasi untuk new_realisasi:", err);
          }
        }

      }).catch((err) => {
        console.error("Gagal memulai LISTEN new_realisasi:", err);
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

  const { closeAll } = require("./pgListener");
  closeAll().catch(() => { }).finally(() => process.exit(0));
});
