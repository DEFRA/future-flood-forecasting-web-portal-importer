const sql = require('mssql')
const { logger } = require('defra-logging-facade')

// async/await style:
const pool = new sql.ConnectionPool(process.env['SQLDB_CONNECTION_STRING'])
const pooledConnect = pool.connect()

pool.on('error', err => {
  logger.error(err)
})

module.exports = { pool, pooledConnect, sql }
