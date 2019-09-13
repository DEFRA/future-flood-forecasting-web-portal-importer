const moment = require('moment')
const axios = require('axios')
const { pool, pooledConnect, sql } = require('../Shared/connection-pool')

module.exports = async function (context, message) {
  // Ensure the connection pool is ready
  await pooledConnect
  const proceedWithImport = await isTaskRunApproved(message)
  if (proceedWithImport) {
    const workflowId = await getWorkflowId(message)
    const locationLookupData = await getLocationLookupData(workflowId, context)
    const timeSeriesDisplayGroupsData = await getTimeseriesDisplayGroups(locationLookupData)
    await loadTimeseriesDisplayGroups(timeSeriesDisplayGroupsData, context)
  } else {
    context.log.warn(`Ignoring unapproved task run completion ${message}`)
  }

  sql.on('error', err => {
    context.log.error(err)
    throw err
  })
  // done() not requried as the async function returns the desired result, there is no output binding to be activated.
  // context.done()
}

async function extract (message, regex, expectedNumberOfMatches, matchIndexToReturn, errorMessageSubject) {
  const errorMessage = `Unable to extract ${errorMessageSubject} from ${message}`
  const matches = regex.exec(message)
  if (matches && matches.length === expectedNumberOfMatches) {
    return matches[matchIndexToReturn]
  } else {
    throw new Error(errorMessage)
  }
}

async function isTaskRunApproved (message) {
  const isMadeCurrentManuallyMessage = 'is made current manually'
  // Test for automatic and manual task run approval.
  const taskRunApprovedRegex = new RegExp(`(?:Approved: )True|False|${isMadeCurrentManuallyMessage}`, 'i')
  const taskRunApprovedText = 'task run approval status'
  const taskRunApprovedString = await extract(message, taskRunApprovedRegex, 1, 0, taskRunApprovedText)
  return !!taskRunApprovedString.match(new RegExp(`true|${isMadeCurrentManuallyMessage}`, 'i'))
}

async function getWorkflowId (message) {
  const workflowIdRegex = /task(?: run)? ([^ ]*) /i
  const workflowIdText = 'workflow ID'
  return extract(message, workflowIdRegex, 2, 1, workflowIdText)
}

async function getLocationLookupData (workflowId, context) {
  let transaction
  let preparedStatement
  let locationLookupData = {}
  try {
    transaction = new sql.Transaction(pool)
    // Run the query to retrieve location lookup data in a read only transaction with a table lock held
    // for the duration of the transaction to guard against a location lookup data refresh during
    // data retrieval.
    await transaction.begin()
    preparedStatement = new sql.PreparedStatement(transaction)

    await preparedStatement.input('workflowId', sql.NVarChar)

    await preparedStatement.prepare(`
      select
        plot_id,
        location_ids
      from
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.location_lookup
      with
       (tablock holdlock)
      where
        workflow_id = @workflowId
    `)

    const parameters = {
      workflowId: workflowId
    }

    const locationLookupResponse = await preparedStatement.execute(parameters)

    for (const record of locationLookupResponse.recordset) {
      locationLookupData[record.plot_id] = record.location_ids
    }

    if (Object.keys(locationLookupData).length === 0) {
      // TO DO - No location lookup data. Insert error record in the staging database.
    }

    return locationLookupData
  } catch (err) {
    try {
      context.log.error(err)
      await transaction.rollback()
      throw err
    } catch (err) {}
  } finally {
    try {
      if (preparedStatement) {
        await preparedStatement.unprepare()
        await transaction.commit()
      }
    } catch (err) { console.error(err) }
  }
}

async function getTimeseriesDisplayGroups (locationLookupData) {
  const now = moment.utc()
  // Retrieve data from the last two days to the next five days by default.
  // This time period can be overridden by the two environment variables
  // FEWS_START_TIME_OFFSET_HOURS and FEWS_END_TIME_OFFSET_HOURS.
  const startTimeOffsetHours = process.env['FEWS_START_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_START_TIME_OFFSET_HOURS']) : 48
  const endTimeOffsetHours = process.env['FEWS_END_TIME_OFFSET_HOURS'] ? parseInt(process.env['FEWS_END_TIME_OFFSET_HOURS']) : 120
  const startTime = moment(now).subtract(startTimeOffsetHours, 'hours').toISOString()
  const endTime = moment(now).add(endTimeOffsetHours, 'hours').toISOString()

  // The database in which data is loaded requires fractional seconds to be included in dates. By contrast
  // the REST interface of the core forecasting engine requires fractional seconds to be excluded from dates.
  const fewsStartTime = `&startTime=${startTime.substring(0, 19)}Z`
  const fewsEndTime = `&endTime=${endTime.substring(0, 19)}Z`

  const data = {
    startTime: startTime,
    endTime: endTime,
    timeseries: []
  }

  for (const key of Object.keys(locationLookupData)) {
    const plotId = `&plotId=${key}`
    const locationIds = `&locationIds=${locationLookupData[key].replace(/;/g, '&locationIds=')}`
    const fewsParameters = `${plotId}${locationIds}${fewsStartTime}${fewsEndTime}`

    // Get the timeseries display groups for the configured plot, locations and date range.
    const fewsPiEndpoint =
     `${process.env['FEWS_PI_API']}/FewsWebServices/rest/fewspiservice/v1/timeseries/displaygroups?useDisplayUnits=false
        &showThresholds=true&omitMissing=true&onlyHeaders=false&documentFormat=PI_JSON${fewsParameters}`

    const fewsResponse = await axios.get(fewsPiEndpoint)
    data.timeseries.push(JSON.stringify(fewsResponse.data))
  }
  return data
}

async function loadTimeseriesDisplayGroups (data, context) {
  const preparedStatement = new sql.PreparedStatement(pool)

  try {
    await preparedStatement.input('id', sql.UniqueIdentifier)
    await preparedStatement.input('timeseries', sql.NVarChar)
    await preparedStatement.input('startTime', sql.DateTime2)
    await preparedStatement.input('endTime', sql.DateTime2)

    await preparedStatement.prepare(`
      insert into
        ${process.env['FFFS_WEB_PORTAL_STAGING_DB_STAGING_SCHEMA']}.timeseries (fews_data, start_time, end_time)
      output
        inserted.id
      values
       (@timeseries, @startTime, @endTime)
    `)

    for (const index in data.timeseries) {
      const parameters = {
        timeseries: data.timeseries[index],
        startTime: data.startTime,
        endTime: data.endTime
      }

      await preparedStatement.execute(parameters)
      // TO DO - Send a message containing the primary key of the new record to a queue.
    }
  } catch (err) {
    context.log.error(err)
    throw err
  } finally {
    try {
      if (preparedStatement) {
        await preparedStatement.unprepare()
      }
    } catch (err) {}
  }
}
