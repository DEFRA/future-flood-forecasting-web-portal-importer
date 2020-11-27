const { doInTransaction, executePreparedStatementInTransaction } = require('../../../Shared/transaction-helper')
const sql = require('mssql')

module.exports = function (context, pool, config) {
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

  this.checkWorkflowRefreshData = async function () {
    await doInTransaction(checkWorkflowRefreshDataInTransaction, context, 'Unable to check workflow refresh data', null, config)
  }
}

async function checkWorkflowRefreshDataInTransaction (transaction, context, config) {
  await executePreparedStatementInTransaction(checkWorkflowRefreshDataInternal, context, transaction, config)
}

async function checkWorkflowRefreshDataInternal (context, preparedStatement, config) {
  await preparedStatement.input('csvType', sql.NVarChar)
  await preparedStatement.prepare(`
    select
      count(id) as number
    from
      fff_staging.workflow_refresh
    where
      csv_type = @csvType and
      refresh_time < getutcdate();
  `)

  const parameters = {
    csvType: config.csvType
  }

  const result = await preparedStatement.execute(parameters)
  expect(result.recordset[0].number).toBe(1)
}
