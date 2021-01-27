# Replaying Messages From The fews-eventcode And fews-import Dead Letter Queues After An Outage

* Disable the ProcessFewsEventCode function (achievable using the Azure portal or by using the function app setting AzureWebJobs.ProcessFewsEventCode.Disabled with a value of true).
* Disable the ImportFromFews function (achievable using the Azure portal or by using the function app setting AzureWebJobs.ImportFromFews.Disabled with a value of true).
* Enable the ReplayProcessFewsEventCode function (achievable using the Azure portal or by changing the value of the function app setting AzureWebJobs.ReplayProcessFewsEventCode.Disabled to false).
* Enable the ReplayImportFromFews function (achievable using the Azure portal or by changing the value of the function app setting AzureWebJobs.ReplayImportFromFews.Disabled to false).
* Wait for all mesages on the fews-eventcode dead letter queue to transfer to the fews-eventcode queue/topic.
* Wait for all mesages on the fews-import dead letter queue to transfer to the fews-import queue/topic.
* Disable the ReplayProcessFewsEventCode function (achievable using the Azure portal or by changing the value of the function app setting AzureWebJobs.ReplayProcessFewsEventCode.Disabled to true).
* Disable the ReplayImportFromFews function (achievable using the Azure portal or by changing the value of the function app setting AzureWebJobs.ReplayImportFromFews.Disabled to true).
* Enable the ImportFromFews function (achievable using the Azure portal or by using the function app setting AzureWebJobs.ImportFromFews.Disabled with a value of false).
* Enable the ProcessFewsEventCode function (achievable using the Azure portal or by using the function app setting AzureWebJobs.ProcessFewsEventCode.Disabled with a value of false).
