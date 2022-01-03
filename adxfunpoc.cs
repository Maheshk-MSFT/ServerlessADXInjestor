using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Ingest;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading.Tasks;

namespace ServerlessADXInjestor
{
    public static class adxfunpoc
    {
        private static KustoConnectionStringBuilder connection;
        private static IKustoQueuedIngestClient adx;
        private static Object _lock = new Object();
        private static bool _initialized = false;

        [FunctionName("Serverless_Func_ADXInjestor")]
        public static async Task Run([BlobTrigger("samples/{name}", Connection = "AzureWebJobsStorage")] Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($" ***** C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes *****");

            if (!TryInitialize(log))
            {
                log.LogError("Could not initialize, cancel request");
                throw new Exception("Could not initialize, cancel request");
            }

            string blobPath = GetEnvVariable("blobPath") + name;
            string sasToken = GetEnvVariable("sastoken");
            string filepath = blobPath + sasToken;
            var databaseName = GetEnvVariable("databaseName"); 
            var table = GetEnvVariable("table"); 
            var tableMapping = GetEnvVariable("tableMapping");

            var properties =
                    new KustoQueuedIngestionProperties(databaseName, table)
                    {
                        Format = DataSourceFormat.csv,
                        IngestionMapping = new IngestionMapping()
                        {
                            IngestionMappingReference = tableMapping,
                            IngestionMappingKind = IngestionMappingKind.Csv
                        },
                        IgnoreFirstRecord = true
                    };
            try
            {
                await TriggerIngestCommand(filepath, properties, log);
            }
            catch (Exception e)
            {
                log.LogError($"Error while trying to insert blob {filepath} because of message: {e.Message}");
                throw e;
            }
            finally
            {
                log.LogTrace($"Triggered insertion of blob {filepath}");
            }
           log.LogInformation("************************************ Success! ************************************|" + name);
        }

        private static Task TriggerIngestCommand(string filepath, KustoIngestionProperties ingestProperties, ILogger log)
        {
            log.LogInformation("TriggerIngestCommand > "+ filepath);

            return adx.IngestFromStorageAsync(filepath, ingestProperties);
        }

        private static string GetEnvVariable(String name)
        {
            return System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
        }

        private static bool TryInitialize(ILogger log)
        {
            lock (_lock)
            {
                if (!_initialized)
                {
                    string kustoIngestUrl = GetEnvVariable("kustoIngestUrl");
                    string clientId = GetEnvVariable("clientId");
                    string clientSecret = GetEnvVariable("clientSecret");
                    string tenantId = GetEnvVariable("tenantId");


                    if (String.IsNullOrWhiteSpace(kustoIngestUrl)
                        || String.IsNullOrWhiteSpace(clientId)
                        || String.IsNullOrWhiteSpace(clientSecret)
                        || String.IsNullOrWhiteSpace(tenantId))
                    {
                        log.LogError($"Could not initialize the Kusto client because the connection parameters are wrong (url: {kustoIngestUrl} clientId: {clientId}, tenant: {tenantId}");

                        return false;
                    }

                    //Initialize adx
                    connection = new KustoConnectionStringBuilder(kustoIngestUrl).WithAadApplicationKeyAuthentication(clientId, clientSecret, tenantId);

                    adx = KustoIngestFactory.CreateQueuedIngestClient(connection);

                    if (adx != null)
                    {
                        _initialized = true;
                        log.LogInformation("Function successfully initialized");

                        return true;
                    }
                    else
                    {
                        log.LogWarning("Function not successfully initialized");
                    }
                }
                else
                {
                    return true;
                }
            }
            return false;
        }
    }
}
