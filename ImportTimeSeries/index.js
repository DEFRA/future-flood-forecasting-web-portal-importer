module.exports = async function (context, message) {
  const axios = require('axios')
  const sql = require('mssql')
  const uuidv4 = require('uuid/v4')

  // This function is triggered via a queue message drop
  context.log('JavaScript queue trigger function processed work item', message)
  context.log(context.bindingData)

  let pool
  let preparedStatement
  try {
    // Get the timeseries for the location identified in the message from the FEWS PI server.
    const fewsPiEndpoint = `${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries?locationIds=${message}&useDisplayUnits=false&showThresholds=true&omitMissing=true&onlyHeaders=false&documentFormat=PI_JSON`
    const fewsResponse = await axios.get(fewsPiEndpoint)
    const timeseries = JSON.stringify(fewsResponse.data)

    // Insert the timeseries into the staging database
    pool = await sql.connect(process.env['SQLDB_CONNECTION_STRING'])
    preparedStatement = new sql.PreparedStatement(pool)
    await preparedStatement.input('id', sql.UniqueIdentifier)
    await preparedStatement.input('timeseries', sql.NVarChar)
    await preparedStatement.prepare('INSERT INTO fffs_staging.timeseries (id, fews_data) VALUES (@id,  @timeseries)')
    const parameters = {
      id: uuidv4(),
      timeseries: timeseries
    }
    await preparedStatement.execute(parameters)
  } catch (err) {
    context.log.error(err)
    throw err
  } finally {
    try {
      if (preparedStatement) {
        await preparedStatement.unprepare()
      }
    } catch (err) { }
    try {
      if (pool) {
        await pool.close()
      }
    } catch (err) { }
    try {
      if (sql) {
        await sql.close()
      }
    } catch (err) { }
  }
  sql.on('error', err => {
    context.log.error(err)
    throw err
  })
}
