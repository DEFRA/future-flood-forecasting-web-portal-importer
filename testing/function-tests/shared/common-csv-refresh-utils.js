import { doInTransaction } from '../../../Shared/transaction-helper.js'
import sql from 'mssql'

export default function (context, pool) {
  this.checkExpectedServiceConfigurationUpdateNotificationStatus = async function (context, expectedServiceConfigurationUpdateNotification) {
    if (expectedServiceConfigurationUpdateNotification) {
      expect(context.bindings.serviceConfigurationUpdateCompleted.length).toBe(1)
    } else {
      expect(context.bindings.serviceConfigurationUpdateCompleted.length).toBe(0)
    }
  }

  this.insertCSVStagingException = async function () {
    const isolationLevel = null
    await doInTransaction({ fn: insertCSVStagingException, context, errorMessage: 'Unable to insert csv staging exception data', isolationLevel })
  }

  this.insertNonWorkflowRefreshRecords = async function (nonWorkflowRefreshOffset) {
    const request = new sql.Request(pool)
    if (nonWorkflowRefreshOffset && Number.isInteger(nonWorkflowRefreshOffset)) {
      request.input('nonWorkflowRefreshOffset', sql.Int, nonWorkflowRefreshOffset)
      await request.batch(`
        insert into
          fff_staging.non_workflow_refresh (csv_type, refresh_time)
        values
          ('CMV', dateadd(second, @nonWorkflowRefreshOffset, getutcdate())),
          ('CTI', dateadd(second, @nonWorkflowRefreshOffset, getutcdate())),
          ('CTR', dateadd(second, @nonWorkflowRefreshOffset, getutcdate())),
          ('FFL', dateadd(second, @nonWorkflowRefreshOffset, getutcdate())),
          ('MVT', dateadd(second, @nonWorkflowRefreshOffset, getutcdate())),
          ('LTH', dateadd(second, @nonWorkflowRefreshOffset, getutcdate())),
          ('TGR', dateadd(second, @nonWorkflowRefreshOffset, getutcdate()))
      `)
    } else {
      await request.batch(`
        insert into
          fff_staging.non_workflow_refresh (csv_type)
        values
          ('CMV'),
          ('CTI'),
          ('CTR'),
          ('FFL'),
          ('MVT'),
          ('LTH'),
          ('TGR')
      `)
    }
  }
}

async function insertCSVStagingException (transaction, context) {
  await new sql.Request(transaction).batch(`
  insert into 
    fff_staging.csv_staging_exception (csv_source_file, row_data, description, exception_time, workflow_id)
  values  
    ('other', 'data', 'test data', getutcdate(), 'workflow')`
  )
}
