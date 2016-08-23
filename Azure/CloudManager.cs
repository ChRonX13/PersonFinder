using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Configuration;

namespace Cloud
{
    public static class CloudManager
    {
        public static CloudQueue GetCloudQueue(string queueReference)
        {
            var queueClient = GetCloudQueueClient();

            return queueClient.GetQueueReference(queueReference);
        }

        private static CloudQueueClient GetCloudQueueClient()
        {
            var storageAccount = GetCloudStorageAccount();

            return storageAccount.CreateCloudQueueClient();
        }

        private static CloudStorageAccount GetCloudStorageAccount()
        {
            return CloudStorageAccount.Parse(GetConnectionString());
        }

        private static string GetConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["StorageConnectionString"].ConnectionString;
        }
    }
}
