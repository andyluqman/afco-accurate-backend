const fs = require("fs");
const path = require("path");
const { Client } = require("minio");

const LOCAL_UPLOAD_DIR = path.join(__dirname, "uploads");

const minioOptions = {
  endPoint: process.env.MINIO_ENDPOINT || "",
  port: Number(process.env.MINIO_PORT || 9000),
  useSSL: String(process.env.MINIO_USE_SSL || "false").toLowerCase() === "true",
  accessKey: process.env.MINIO_ACCESS_KEY || "",
  secretKey: process.env.MINIO_SECRET_KEY || "",
  region: process.env.MINIO_REGION || undefined,
};

const BUCKET = process.env.MINIO_BUCKET || "accurate-uploads";
const OBJECT_PREFIX = process.env.MINIO_PREFIX || "uploads/";

const isMinioEnabled = Boolean(
  minioOptions.endPoint && minioOptions.accessKey && minioOptions.secretKey
);

const minioClient = isMinioEnabled
  ? new Client({
    endPoint: minioOptions.endPoint,
    port: minioOptions.port,
    useSSL: minioOptions.useSSL,
    accessKey: minioOptions.accessKey,
    secretKey: minioOptions.secretKey,
    region: minioOptions.region,
  })
  : null;

let bucketReady = null;
const ALLOWED_FOLDERS = ["upload", "uploads", "foto", "karyawan", "pindah-barang", "dokumen", "ttd", "berkas", "gambar","terima_pb"];

function ensureBucket() {
  if (!isMinioEnabled) return Promise.resolve(false);
  if (!bucketReady) {
    bucketReady = (async () => {
      const exists = await minioClient.bucketExists(BUCKET);
      if (!exists) {
        await minioClient.makeBucket(BUCKET, minioOptions.region);
        console.log(`Bucket MinIO "${BUCKET}" dibuat.`);
      }
      return true;
    })().catch((err) => {
      bucketReady = null;
      throw err;
    });
  }
  return bucketReady;
}



function normalizeFolder(folder) {
  if (!folder) return "";
  const f = String(folder).trim().replace(/\\/g, "/");
  if (f.includes("..") || f.startsWith("/") || f.includes("://")) return "";
  const clean = f.split("/").filter(Boolean).join("/");
  if (!clean) return "";
  if (!/^[a-zA-Z0-9/_-]+$/.test(clean)) return "";
  // Only allow first level folder or predefined paths
  const first = clean.split("/")[0].toLowerCase();
  if (ALLOWED_FOLDERS.includes(first)) {
    // normalize to lowercase for first level
    const parts = clean.split("/");
    parts[0] = first;
    return parts.join("/");
  }
  return "";
}

function buildObjectKey(objectName, folder = "") {
  const normFolder = normalizeFolder(folder);
  if (normFolder) {
    return `${OBJECT_PREFIX}${normFolder}/${objectName}`;
  }
  return `${OBJECT_PREFIX}${objectName}`;
}

function stripObjectKey(key) {
  if (key.startsWith(OBJECT_PREFIX)) {
    return key.slice(OBJECT_PREFIX.length);
  }
  return key;
}

function isNotFoundError(err) {
  return (
    err &&
    (err.code === "NoSuchKey" ||
      err.code === "NotFound" ||
      err.code === "NoSuchBucket" ||
      err.statusCode === 404)
  );
}

// Simpan file (image) ke MinIO. Kalau MinIO tidak dikonfigurasi, fallback ke disk lokal.
async function putFile(objectName, buffer, contentType, folder = "") {
  const normFolder = normalizeFolder(folder);
  if (isMinioEnabled) {
    await ensureBucket();
    const key = buildObjectKey(objectName, normFolder);
    const result = await minioClient.putObject(
      BUCKET,
      key,
      buffer,
      buffer.length,
      { "Content-Type": contentType || "application/octet-stream" }
    );
    const relative = normFolder ? `${normFolder}/${objectName}` : objectName;
    return {
      storage: "minio",
      objectName,
      relative,
      folder: normFolder,
      key,
      etag: result.etag,
      size: buffer.length,
    };
  }

  const dir = normFolder ? path.join(LOCAL_UPLOAD_DIR, normFolder) : LOCAL_UPLOAD_DIR;
  await fs.promises.mkdir(dir, { recursive: true });
  await fs.promises.writeFile(path.join(dir, objectName), buffer);
  const relative = normFolder ? `${normFolder}/${objectName}` : objectName;
  return {
    storage: "local",
    objectName,
    relative,
    folder: normFolder,
    key: relative,
    etag: null,
    size: buffer.length,
  };
}

async function statFile(objectName, folder = "") {
  const normFolder = normalizeFolder(folder);
  const key = buildObjectKey(objectName, normFolder);
  if (isMinioEnabled) {
    try {
      const stat = await minioClient.statObject(BUCKET, key);
      return {
        size: stat.size,
        contentType: stat.metaData?.["content-type"] || null,
        etag: stat.etag,
        lastModified: stat.lastModified,
      };
    } catch (err) {
      if (isNotFoundError(err)) return null;
      throw err;
    }
  }

  try {
    const filePath = normFolder
      ? path.join(LOCAL_UPLOAD_DIR, normFolder, path.basename(objectName))
      : path.join(LOCAL_UPLOAD_DIR, path.basename(objectName));
    const stat = await fs.promises.stat(filePath);
    if (!stat.isFile()) return null;
    return { size: stat.size, contentType: null, etag: null, lastModified: stat.mtime };
  } catch {
    return null;
  }
}

// Stream file dari MinIO (atau disk lokal) untuk dikirim ke client.
async function getFileStream(objectName, folder = "") {
  const normFolder = normalizeFolder(folder);
  if (isMinioEnabled) {
    return minioClient.getObject(BUCKET, buildObjectKey(objectName, normFolder));
  }

  const filePath = normFolder
    ? path.join(LOCAL_UPLOAD_DIR, normFolder, path.basename(objectName))
    : path.join(LOCAL_UPLOAD_DIR, path.basename(objectName));
  await fs.promises.access(filePath);
  return fs.createReadStream(filePath);
}

async function removeFile(objectName, folder = "") {
  const normFolder = normalizeFolder(folder);
  if (isMinioEnabled) {
    await minioClient.removeObject(BUCKET, buildObjectKey(objectName, normFolder));
    return true;
  }

  const filePath = normFolder
    ? path.join(LOCAL_UPLOAD_DIR, normFolder, path.basename(objectName))
    : path.join(LOCAL_UPLOAD_DIR, path.basename(objectName));
  await fs.promises.unlink(filePath).catch(() => { });
  return true;
}

// Daftar nama file (tanpa prefix) di dalam folder upload.
// namePrefix optional: difilter di sisi server (MinIO) supaya tidak listing seluruh bucket.
async function listFiles(namePrefix = "", folder = "") {
  const normFolder = normalizeFolder(folder);
  if (isMinioEnabled) {
    const names = [];
    const prefix = normFolder
      ? `${OBJECT_PREFIX}${normFolder}/${namePrefix}`
      : `${OBJECT_PREFIX}${namePrefix}`;
    const stream = minioClient.listObjectsV2(BUCKET, prefix, true);
    await new Promise((resolve, reject) => {
      stream.on("data", (obj) => names.push(stripObjectKey(obj.name)));
      stream.on("end", resolve);
      stream.on("error", reject);
    });
    return names;
  }

  try {
    const dir = normFolder ? path.join(LOCAL_UPLOAD_DIR, normFolder) : LOCAL_UPLOAD_DIR;
    const files = await fs.promises.readdir(dir);
    return files.filter(
      (f) => !f.startsWith(".") && (!namePrefix || f.startsWith(namePrefix))
    );
  } catch {
    return [];
  }
}

// Cari file terbaru dengan pola <baseName>_ (dipakai sebagai fallback saat file persis tidak ada).
async function findLatestFile(baseName, folder = "") {
  const files = await listFiles(baseName + "_", folder);
  const matches = files.filter((f) => f.startsWith(baseName + "_"));
  if (matches.length === 0) {
    const exact = await statFile(baseName, folder);
    return exact ? baseName : null;
  }
  matches.sort((a, b) => b.localeCompare(a));
  return matches[0];
}

// URL presigned untuk akses langsung ke object MinIO.
async function getPresignedUrl(objectName, expirySeconds = 3600, folder = "") {
  if (!isMinioEnabled) return null;
  await ensureBucket();
  return minioClient.presignedGetObject(
    BUCKET,
    buildObjectKey(objectName, folder),
    expirySeconds
  );
}

module.exports = {
  isMinioEnabled,
  BUCKET,
  OBJECT_PREFIX,
  ALLOWED_FOLDERS,
  ensureBucket,
  normalizeFolder,
  putFile,
  statFile,
  getFileStream,
  removeFile,
  listFiles,
  findLatestFile,
  getPresignedUrl,
};
