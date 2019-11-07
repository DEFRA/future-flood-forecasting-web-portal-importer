const sql = require('mssql')
const { logger } = require('defra-logging-facade')

// async/await style:
const pool = new sql.ConnectionPool(process.env['SQLDB_CONNECTION_STRING'])

async function pooledConnect (pool) {
  try {
    await pool.connect()

    return pool
  } catch (err) {
    logger.error(err)
  }
}

pool.on('error', err => {
  logger.error(err)
})

module.exports = { pool, pooledConnect, sql }
