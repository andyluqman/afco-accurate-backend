require('dotenv').config();
const { Pool } = require('pg');

// ===== POSTGRESQL SETUP =====
const pgPool = new Pool({
    // PostgreSQL config
    host: process.env.PG_HOST,
    port: process.env.PG_PORT,
    user: process.env.PG_USER,
    password: process.env.PG_PASS,
    database: process.env.PG_DB,
  });

module.exports = {
  pgPool
};
