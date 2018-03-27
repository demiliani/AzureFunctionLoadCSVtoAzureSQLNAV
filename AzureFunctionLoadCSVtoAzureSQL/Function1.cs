using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;

using NAVWSDAL;

namespace AzureFunctionLoadCSVtoAzureSQL
{
    public static class Function1
    {
        [FunctionName("LoadCSVZipFileToAzureSQL")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, TraceWriter log) //Runs every 5 minutes
        {
            log.Info($"C# Timer trigger function executed at: {DateTime.Now}");

            log.Info("Downloading ZIP archive from Azure Storage...");

            ReadBlobStorage();

            log.Info("ZIP archive downloaded successfully.");

            log.Info("Extracting ZIP archive...");

            var d = Directory.CreateDirectory($"{Path.GetTempPath()}\\orders_unzip");

            Directory.Delete(d.FullName, true);
            d = Directory.CreateDirectory($"{Path.GetTempPath()}\\orders_unzip");
            ZipFile.ExtractToDirectory($"{Path.GetTempPath()}\\orders.zip", d.FullName);

            log.Info("ZIP archive extracted.");

            var filename = d.GetFiles("*.csv")[0].FullName;

            log.Info($"CSV filename: {filename}");

            DataTable sourceData = CreateDataTable(filename);

            log.Info($"Data loaded from CSV");

            log.Info("Writing data to Azure SQL");

            //Write the data to NAV
            WriteToNAV(sourceData);

            log.Info("Process terminated successfully.");
        }

        private static void ReadBlobStorage()
        {
            //var storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=;AccountKey===");
            var storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["AzureWebJobsStorage"]);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference("order_container");
            var blob = container.GetBlockBlobReference("Orders.zip");
            blob.DownloadToFileAsync($"{Path.GetTempPath()}\\orders.zip", FileMode.CreateNew);
        }

        private static DataTable CreateDataTable(string filename)
        {
            DataTable csvData = new DataTable();

            csvData.Columns.Add("OrderNo");
            csvData.Columns.Add("CustomerNo");
            csvData.Columns.Add("ItemNo");
            csvData.Columns.Add("Quantity");

            using (var rd = new StreamReader(filename))
            {
                while (!rd.EndOfStream)
                {
                    var fields = rd.ReadLine().Split(',');
                    csvData.Rows.Add(fields[0], fields[1], fields[2], fields[3]);
                }
            }

            return csvData;
        }

        private static void WriteToNAV(DataTable data)
        {
            SqlBulkCopy bcp = new SqlBulkCopy(ConfigurationManager.AppSettings["AzureSQLConnectionString"]);
            //SqlBulkCopy bcp = new SqlBulkCopy("Server=tcp:.database.windows.net,1433;Initial Catalog=;Persist Security Info=False;User ID=;Password=;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;");
            bcp.DestinationTableName = "External_Orders";
            bcp.WriteToServer(data);

            //Call NAV WS
            NAVWSDAL.NAVWSDAL d = new NAVWSDAL.NAVWSDAL();
            d.CallNAVWS(data);
        }
    }

    
}
