import getBooleanIndicator from './get-boolean-indicator.js'

export default async function (context, taskRunData) {
  return getBooleanIndicator(context, taskRunData, 'Forecast')
}
