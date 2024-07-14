import { getDuration } from '../utils.js'
import moment from 'moment'

const maximumDelayForPiServerDataAvailabilityAfterTaskRunCompletionConfig = {
  environmentVariableName: 'MAXIMUM_DELAY_FOR_DATA_AVAILABILITY_AFTER_TASK_RUN_COMPLETION_MILLIS',
  defaultDuration: 120000
}

const MAXIMUM_NUMBER_OF_MILLISECONDS_AFTER_TASK_RUN_COMPLETION_TO_ALLOW_FOR_PI_SERVER_DATA_AVAILABILITY =
  getDuration(maximumDelayForPiServerDataAvailabilityAfterTaskRunCompletionConfig)

const NO_ACTION_REASON =
 `the task run completed more than ${MAXIMUM_NUMBER_OF_MILLISECONDS_AFTER_TASK_RUN_COMPLETION_TO_ALLOW_FOR_PI_SERVER_DATA_AVAILABILITY / 1000} second(s) ago`

export default async function (config, ...args) {
  const context = config.context
  const millisecondsSinceTaskRunCompletion =
    moment.utc().diff(moment.utc(new Date(`${config.taskRunData.taskRunCompletionTime}`)), 'milliseconds')

  if (millisecondsSinceTaskRunCompletion < MAXIMUM_NUMBER_OF_MILLISECONDS_AFTER_TASK_RUN_COMPLETION_TO_ALLOW_FOR_PI_SERVER_DATA_AVAILABILITY) {
    await config.fn(context, config.taskRunData, ...args)
  } else {
    context.log.warn(`${config.noActionTakenMessage} because ${NO_ACTION_REASON}`)
  }
}
