const { callProcedurePg } = require("./dbPg");
const firebird = require('node-firebird');
require('dotenv').config();
const { decryptData } = require("./utils");
const {cleanupPools} = require("./dbFbPoolManager")

const dbConfigCache = {};

function loadAllDbConfigs() {
  return new Promise((resolve, reject) => {
    console.log("loadalldbconfig")
    callProcedurePg('accurate.settingdatabase_manage',['6def3da0-ec48-4c49-8f40-be96201367f4', '/databases', 'VIEW', null, '', '', '', '', '', '', '', '', '', '', '', process.env.FB_KEY])
      .then(result => {
        // Reset isi cache (ingat: ini harus pakai let atau delete tiap key kalau pakai const)
        for (const key in dbConfigCache) {
          delete dbConfigCache[key];
        }
        console.log("reset dbConfigCache");
        for (let i = 1; i < result[1].length; i++) {
          const row = result[1][i];

          const name = row[1];
          const host = decryptData(row[3], "$2a$06$HDmt9c75ascR9vn/jozSQO");
          const port = decryptData(row[4], "$2a$06$HDmt9c75ascR9vn/jozSQO");
          const database = decryptData(row[5], "$2a$06$HDmt9c75ascR9vn/jozSQO");
          const user = decryptData(row[6], "$2a$06$HDmt9c75ascR9vn/jozSQO");
          const pass = decryptData(row[7], "$2a$06$HDmt9c75ascR9vn/jozSQO");
          const isdev = row[8];

          dbConfigCache[name] = {
            host,
            port,
            database,
            user,
            password: pass,
            isdev
          };
        }
        cleanupPools();
        resolve(result[1].length-1); // ✅ Penting: tandai bahwa promise sudah selesai
      })
      .catch(err => {
        console.error('Error call procedure:', err);
        reject(err); // ✅ Penting: reject jika ada error
      });
  });
}

function getDbConfig(name) {
  return dbConfigCache[name];
}

// Function khusus untuk mendapatkan nama database saja
function getDatabaseNames() {
  return Object.keys(dbConfigCache);
}

module.exports = {
  loadAllDbConfigs,
  getDbConfig,
  getDatabaseNames,
  dbConfigCache
};
