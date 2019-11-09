const sql = require('mssql')
const { logger } = require('defra-logging-facade')

module.exports = function () {
  this.pool = new sql.ConnectionPool(process.env['SQLDB_CONNECTION_STRING'])
  this.pooledConnect = this.pool.connect()
  this.pool.on('error', err => {
    logger.error(err)
  })
}
