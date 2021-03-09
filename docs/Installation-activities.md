# Installation Activities

The following activities need to be performed for the functions to run. While the documentation states what activities need to be performed it
does not prescribe how the activities should be performed.

* Configure app settings/environment variables
* Install node modules
* If running locally, install Azure Functions Core Tools (includes a version of the same runtime that powers Azure functions runtime that you can run locally. It also provides commands to create functions, connect to Azure and deploy function projects).
* Install function extensions. Extension bundles is a deployment technology that lets you add a compatible set of Functions binding extensions to your function app. A predefined set of extensions are added when you build your app. Extension packages defined in a bundle are compatible with each other, which helps you avoid conflicts between packages. You enable extension bundles in the app's host.json file. You can use extension bundles with version 2.x and later versions of the Functions runtime. When developing locally, make sure you are using the latest version of [Azure Functions Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local#v3 "Microsoft Azure documentation"). If you don't use extension bundles, you must install the .NET Core 2.x SDK on your local computer before you install any binding extensions. Extension bundles removes this requirement for local development.
* Run npm scripts to configure the functions and run unit tests. For example:
  * npm run build && npm test
* To run the functions in a Microsoft Azure environment, deploy the functions to a function app
* To run the functions locally, issue the command **func start** as described in the Microsoft Azure documentation referenced above.  
