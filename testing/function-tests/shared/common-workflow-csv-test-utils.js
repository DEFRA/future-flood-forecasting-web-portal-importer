import { doInTransaction, executePreparedStatementInTransaction } from '../../../Shared/transaction-helper.js'
import sql from 'mssql'
import { expect } from 'vitest'

export default function (context, pool, config) {
  this.insertWorkflowRefreshRecords = async function (workflowRefreshOffset) {
    const request = new sql.Request(pool)
    if (workflowRefreshOffset && Number.isInteger(workflowRefreshOffset)) {
      request.input('workflowRefreshOffset', sql.Int, workflowRefreshOffset)
      await request.batch(`
        insert into
          fff_staging.workflow_refresh (csv_type, refresh_time)
        values
          ('C', dateadd(second, @workflowRefreshOffset, getutcdate())),
          ('F', dateadd(second, @workflowRefreshOffset, getutcdate())),
          ('N', dateadd(second, @workflowRefreshOffset, getutcdate())),
          ('I', dateadd(second, @workflowRefreshOffset, getutcdate()))
      `)
    } else {
      await request.batch(`
        insert into
          fff_staging.workflow_refresh (csv_type)
        values
          ('C'),
          ('F'),
          ('N'),
          ('I')
      `)
    }
  }

  this.checkWorkflowRefreshData = async function (expectWorkflowRefresh) {
    const isolationLevel = null
    await doInTransaction({ fn: checkWorkflowRefreshDataInTransaction, context, errorMessage: 'Unable to check workflow refresh data', isolationLevel }, config, expectWorkflowRefresh)
  }

  this.checkReplayedStagingExceptionMessages = async function (expectedReplayedStagingExceptionMessages) {
    expect(context.bindings.processFewsEventCode.length).toBe((expectedReplayedStagingExceptionMessages || []).length)
    for (const stagingExceptionMessage of expectedReplayedStagingExceptionMessages || []) {
      expect(context.bindings.processFewsEventCode).toContainEqual(stagingExceptionMessage)
    }
  }
  this.checkReplayedTimeseriesStagingExceptionMessages = async function (expectedReplayedTimeseriesStagingExceptionMessages) {
    expect(context.bindings.importFromFews.length).toBe((expectedReplayedTimeseriesStagingExceptionMessages || []).length)
    for (const timeseriesStagingExceptionMessage of expectedReplayedTimeseriesStagingExceptionMessages || []) {
      expect(context.bindings.importFromFews).toContainEqual(timeseriesStagingExceptionMessage)
    }
  }
}

async function checkWorkflowRefreshDataInTransaction (transaction, context, config, expectWorkflowRefresh) {
  await executePreparedStatementInTransaction(checkWorkflowRefreshDataInternal, context, transaction, config, expectWorkflowRefresh)
}

async function checkWorkflowRefreshDataInternal (context, preparedStatement, config, expectWorkflowRefresh) {
  await preparedStatement.input('csvType', sql.NVarChar)
  await preparedStatement.prepare(`
    select
      count(id) as number
    from
      fff_staging.workflow_refresh
    where
      csv_type = @csvType and
      refresh_time < getutcdate()
  `)

  const parameters = {
    csvType: config.csvType
  }

  const result = await preparedStatement.execute(parameters)
  if (expectWorkflowRefresh === true) {
    expect(result.recordset[0].number).toBe(1)
  } else {
    expect(result.recordset[0].number).toBe(0)
  }
}
