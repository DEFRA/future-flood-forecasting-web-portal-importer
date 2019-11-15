const sql = require('mssql')
const { logger } = require('defra-logging-facade')

module.exports = function () {
  this.pool = new sql.ConnectionPool(process.env['SQLDB_CONNECTION_STRING'])

  // To catch critical pool failures
  this.pool.on('error', err => {
    logger.error(err)
    throw err
  })
}
