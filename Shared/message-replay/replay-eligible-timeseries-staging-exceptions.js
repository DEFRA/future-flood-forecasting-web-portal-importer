import { executePreparedStatementInTransaction } from '../transaction-helper.js'
import { prepareServiceConfigurationUpdateDetectionQueryForWorkflowCsvData } from '../csv-load/service-configuration-update-utils.js'
import sql from 'mssql'

const activeTimeseriesStagingExceptionMessagesByCsvTypeForExistingWorkflowsQuery = `
  select distinct
    tse.payload as message
  from
    fff_staging.v_active_timeseries_staging_exception tse
    inner join fff_staging.v_workflow vw
      on tse.source_id = vw.source_id and
      tse.source_type = vw.source_type and
      tse.csv_type = vw.csv_type
    inner join fff_staging.timeseries_header th
      on tse.timeseries_header_id = th.id and
      vw.workflow_id = th.workflow_id
  where
    tse.csv_error = 1 and
    tse.csv_type = @csvType
  union
  select distinct
    tse.payload as message
  from
    fff_staging.v_active_timeseries_staging_exception tse
    inner join fff_staging.timeseries_header th
      on tse.timeseries_header_id = th.id
  where
    tse.csv_error = 1 and
    tse.csv_type = 'U'
`
const timeseriesHeaderMessagesForActiveTimeseriesStagingExceptionsByCsvTypeForMissingWorkflowsQuery = `
  select distinct
    th.message
  from
    fff_staging.v_active_timeseries_staging_exception tse
    inner join fff_staging.timeseries_header th
      on tse.timeseries_header_id = th.id
  where
    tse.csv_error = 1 and
    tse.csv_type = @csvType and
    not exists (
      select
        1
      from
        fff_staging.v_workflow vw
      where
        tse.source_id = vw.source_id and
        tse.source_type = vw.source_type and
        tse.csv_type = vw.csv_type
    )
`

export default async function (context, replayData) {
  // Replay messages for CSV related timeseries staging exceptions linked to known workflows.
  await executePreparedStatementInTransaction(replayMessagesForCsvRelatedTimeseriesStagingExceptions, context, replayData.transaction, replayData)
  // Replay timeseries header (core engine) messages for CSV related timeseries staging exceptions linked to unknown workflows.
  // This should allow processing of data for CSV rows containing corrected typos in plot IDs and filter IDs.
  // Note - This will cause attempted repeat processing of all current plots and filters for the task run identified
  // in the core engine message. However, all plots and filters that have been processed successfully already should
  // be ignored.
  await executePreparedStatementInTransaction(replayTimeseriesHeaderMessagesForEligibleTimeseriesStagingExceptions, context, replayData.transaction, replayData)
  // Replay messages for non-CSV related timeseries staging exceptions if a service configuration update has been processed.
  await executePreparedStatementInTransaction(replayMessagesForTimeseriesStagingExceptionsIfServiceConfigUpdateHasBeenProcessed, context, replayData.transaction)
}

async function replayMessagesForTimeseriesStagingExceptionsIfServiceConfigUpdateHasBeenProcessed (context, preparedStatement) {
  await prepareServiceConfigurationUpdateDetectionQueryForWorkflowCsvData(context, preparedStatement)

  const config = {
    outputBindingName: 'importFromFews',
    parameters: {
      secondsSinceCsvRefreshed: process.env.SERVICE_CONFIG_UPDATE_DETECTION_LIMIT || 300
    },
    parseMessageAsJson: true,
    preparedStatement
  }

  await replayMessagesForEligibleTimeseriesStagingExceptions(context, config)
}

async function replayMessagesForCsvRelatedTimeseriesStagingExceptions (context, preparedStatement, replayData) {
  await preparedStatement.input('csvType', sql.NVarChar)
  await preparedStatement.prepare(activeTimeseriesStagingExceptionMessagesByCsvTypeForExistingWorkflowsQuery)

  const config = {
    outputBindingName: 'importFromFews',
    parameters: {
      csvType: replayData.csvType
    },
    parseMessageAsJson: true,
    preparedStatement
  }

  await replayMessagesForEligibleTimeseriesStagingExceptions(context, config)
}

async function replayMessagesForEligibleTimeseriesStagingExceptions (context, config) {
  const result = await config.preparedStatement.execute(config.parameters)

  for (const record of result.recordset) {
    const messageToReplay = config.parseMessageAsJson ? JSON.parse(record.message) : record.message
    context.bindings[config.outputBindingName].push(messageToReplay)
  }
}

async function replayTimeseriesHeaderMessagesForEligibleTimeseriesStagingExceptions (context, preparedStatement, parameters) {
  await preparedStatement.input('csvType', sql.NVarChar)
  await preparedStatement.prepare(timeseriesHeaderMessagesForActiveTimeseriesStagingExceptionsByCsvTypeForMissingWorkflowsQuery)

  const config = {
    outputBindingName: 'processFewsEventCode',
    parameters,
    preparedStatement
  }

  await replayMessagesForEligibleTimeseriesStagingExceptions(context, config)
}
