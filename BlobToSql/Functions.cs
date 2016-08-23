using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using CsvHelper;
using Ionic.Zip;
using Microsoft.Azure.WebJobs;
using System.Configuration;
using System.Linq;
using Cloud;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;

namespace BlobToSql
{
    public class Functions
    {
        private const int ProcessingSize = 100000;

        public static void ProcessBlob([BlobTrigger("datasets/{name}.zip")] Stream input, string name, TextWriter log)
        {
            log.WriteLine("Processing Zip File: {0}", name);
            Stopwatch watch = Stopwatch.StartNew();

            var blobToSqlQueue = CloudManager.GetCloudQueue("blobtosql");
            blobToSqlQueue.CreateIfNotExists();

            var sqlToStorageQueue = CloudManager.GetCloudQueue("sqltostorage");
            sqlToStorageQueue.CreateIfNotExists();

            var peekMessage = blobToSqlQueue.PeekMessage();

            int batchNumber = 1;

            if (peekMessage != null)
            {
                var batch = JsonConvert.DeserializeObject<Batch>(peekMessage.AsString);
                batchNumber = batch.BatchNumber;
            }

            using (var zip = new ZipInputStream(input))
            {
                zip.GetNextEntry();

                using (var sr = new StreamReader(zip))
                {
                    using (var csv = new CsvReader(sr))
                    {
                        using (var sqlConnection = new SqlConnection(GetConnectionString()))
                        {
                            sqlConnection.Open();
                            csv.Configuration.HasHeaderRecord = false;

                            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                            csv.GetRecords<Person>().Skip(ProcessingSize * (batchNumber - 1));

                            while (batchNumber < 2422)
                            {
                                if (ProcessBatchNumber(batchNumber, csv, sqlConnection, log))
                                {
                                    CreateSqlToStorageMessages(sqlToStorageQueue, batchNumber);

                                    batchNumber++;

                                    UpdateCurrentBatchNumber(blobToSqlQueue, batchNumber);
                                }
                            }
                        }
                    }
                }
            }

            log.WriteLine("Finished operation in {0} seconds", $"{watch.Elapsed.TotalSeconds:0.0}");
        }

        private static void CreateSqlToStorageMessages(CloudQueue queue, int batchNumber)
        {
            for (int i = 1; i < ProcessingSize; i += 10000)
            {
                var offset = batchNumber * ProcessingSize - ProcessingSize;
                var rangeMessage = new CloudQueueMessage(JsonConvert.SerializeObject(new Range
                {
                    StartRange = i + offset,
                    EndRange = i + offset + 9999
                }));

                queue.AddMessage(rangeMessage);
            }
        }

        private static void UpdateCurrentBatchNumber(CloudQueue queue, int batchNumber)
        {
            var message = queue.GetMessage();
            if (message != null)
            {
                queue.DeleteMessage(message);
            }

            var newMessage =
                new CloudQueueMessage(
                    JsonConvert.SerializeObject(new Batch {BatchNumber = batchNumber}));

            queue.AddMessage(newMessage);
        }

        private static bool ProcessBatchNumber(int batchNumber, CsvReader csv, SqlConnection sqlConnection, TextWriter log)
        {
                Stopwatch watch = Stopwatch.StartNew();

                var dataTable = CreateDataTable("Person");

                log.WriteLine("Creating datarows");

                var people = csv.GetRecords<Person>().Take(ProcessingSize);

                foreach (var person in people)
                {
                    dataTable.Rows.Add(person.Id, person.Firstname, person.Surname);
                }

                log.WriteLine("Reading from blob {0} records, {1} per second, in {2} seconds", ProcessingSize,
                    $"{ProcessingSize / watch.Elapsed.TotalSeconds:0.0}", $"{watch.Elapsed.TotalSeconds:0.0}");

                var bulkCopyWatch = Stopwatch.StartNew();

                BulkCopy(sqlConnection, dataTable);

                log.WriteLine("SQL Bulk Copy {0} records, {1} per second, in {2} seconds", ProcessingSize,
                    $"{ProcessingSize / bulkCopyWatch.Elapsed.TotalSeconds:0.0}", $"{bulkCopyWatch.Elapsed.TotalSeconds:0.0}");

                log.WriteLine("Finished batch {0} in {1} seconds", batchNumber, $"{watch.Elapsed.TotalSeconds:0.0}");

                return true;
        }

        private static void BulkCopy(SqlConnection sqlConnection, DataTable dataTable, int batchSize = 20000)
        {
            using (var sqlBulkCopy = new SqlBulkCopy(sqlConnection))
            {
                sqlBulkCopy.DestinationTableName = dataTable.TableName;
                sqlBulkCopy.BatchSize = batchSize;
                sqlBulkCopy.WriteToServer(dataTable);
            }
        }

        private static DataTable CreateDataTable(string name)
        {
            var dataTable = new DataTable(name);

            DataColumn[] columns =
            {
                    new DataColumn("Id", typeof(int)),
                    new DataColumn("Surname", typeof(string)),
                    new DataColumn("Firstname", typeof(string))
                };

            dataTable.Columns.AddRange(columns);
            dataTable.PrimaryKey = new[] { dataTable.Columns["Id"] };

            return dataTable;
        }

        private static string GetConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["PersonFinder"].ConnectionString;
        }


    }
}
