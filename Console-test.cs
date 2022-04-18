using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            var tenantId = "72f988bf-86f1******";
            var kustoUri = "https://mik**adx.centralindia.kusto.windows.net";
            var kustoConnectionStringBuilder = new KustoConnectionStringBuilder(kustoUri).WithAadUserPromptAuthentication(tenantId);
            var blobPath = "https://mikfilesharing.blob.core.windows.net/test/samples/StormEvents_details-ftp_v1.0_d1950_c20210803.csv?23234324";
            var databaseName = "mikkydb";
            var table = "StormEvents1";

            var tableMapping = "StormEvents_CSV_Mapping";

            var ingestUri = "https://ingest-mikk***dx.centralindia.kusto.windows.net";
            var ingestConnectionStringBuilder = new KustoConnectionStringBuilder(ingestUri).WithAadUserPromptAuthentication(tenantId);

            Console.WriteLine("Startigng ADX Dotnet Injestion Client.....");

            #region Actual injestion happens here
            using (var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestConnectionStringBuilder))
            {
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
                Console.WriteLine("Injesting........");

                try
                {
                    IKustoIngestionResult  result = ingestClient.IngestFromStorageAsync(blobPath, ingestionProperties: properties).GetAwaiter().GetResult() as IKustoIngestionResult;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }

                
            }

            Console.WriteLine("Done........");
            #endregion

            #region injestion validator

            using (var cslQueryProvider = KustoClientFactory.CreateCslQueryProvider(kustoConnectionStringBuilder))
            {
                Console.WriteLine("Validating...... ");

                var query = $"{table} | count";

                var results = cslQueryProvider.ExecuteQuery<long>(databaseName, query);
                Console.WriteLine(results.Single());
                Console.WriteLine("Successfully validated from .NET Client..... ");
            }
            #endregion 
        }
    }
}
