"# ServerlessADXInjestor" 
"# ServerlessADXInjestor" 

You can use this code (Azure Function App) having a blob trigger injesting data into Azure Data Explorer using ADX C# SDK. By default, ADX DB provides event grid hooking to Azure Blob storage for automatic injestion upon blob creation/upload.  

When to use this, 

a. When you've limitation with ADX Eventgrid with Storage
b. You may want to branch the code path based on the incoming blob data/extension forwarding to desired DB in ADX
c. When you want to rewrite the path of insertion
d. When you want to ignore the trigger for certain files/formats - etc 
e. When you want to validate certain things before injesting into ADX. 

screenshots - https://twitter.com/MahesKBlr/status/1477938034964197377
