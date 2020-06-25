using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace DurableFunction_SBTrigger
{
    public static class ProcessSBMessages
    {
        static string lockToken;
        static string msgId;
        static MessageReceiver msgReceiver;

        [FunctionName("ProcessSBMessages")]
        public static async Task<string> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            try
            {
                var msg = context.GetInput<NewMessage>();

                //Can be replaced with an activity function that calls API Management to get back message body
                var retMsg = new NewMessage
                {
                    Message = $"This is from a function at {context.CurrentUtcDateTime}"
                };

                //log.LogInformation("Calling write message properties");
                //await context.CallActivityAsync(nameof(GetMessageProperties), msg);

                // 1st Activity that creates a new Blob Message
                log.LogInformation("Calling write to Blob");
                log.LogInformation($"Message: {JsonConvert.SerializeObject(retMsg)}");
                var blobPath = await context.CallActivityAsync<string>(nameof(WriteBlob), retMsg);

                //2nd Activity that writes a message to the SB with the blob path
                //Also simulates a long running process
                log.LogInformation("Calling write to SB");
                var newMsg = await context.CallActivityAsync<Message>(nameof(WriteMessagesToSB), blobPath);

                if (string.IsNullOrEmpty(blobPath) && newMsg != null)
                {
                    log.LogInformation("Complete message");
                    await msgReceiver.CompleteAsync(lockToken);
                    log.LogInformation($"Message Completed.");
                }

                return "Success";
            }
            catch (Exception ex)
            {
                log.LogInformation($"Deadletter message:{ex.Message}");
                await msgReceiver.DeadLetterAsync(lockToken, $"Error occured: {ex.Message}");
                log.LogInformation($"Message Abandoned.");
                return "Fail";
            }
        }

        [FunctionName(nameof(WriteBlob))]
        public static async Task<string> WriteBlob([ActivityTrigger] IDurableActivityContext writeBlobContext, ILogger log)
        {
            BlobContainerClient containerClient = null;
            var newMsg = writeBlobContext.GetInput<NewMessage>();

            //log.LogInformation($"Message: {JsonConvert.SerializeObject(newMsg)}");
            //log.LogInformation($"MessageId:{msgId}");

            // Create a BlobServiceClient object which will be used to create a container client
            BlobServiceClient blobServiceClient = new BlobServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));

            //Create a container if it does not exist
            string containerName = "durablefunctest";
            if (blobServiceClient.GetBlobContainers().Where(cont => cont.Name.ToLowerInvariant() == containerName).ToList().Count() > 0)
            {
                containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            }
            else
            {
                // Create the container and return a container client object
                containerClient = blobServiceClient.CreateBlobContainer(containerName);
            }

            // Get a reference to a blob
            BlobClient blobClient = containerClient.GetBlobClient(msgId);

            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(newMsg))))
            {
                await blobClient.UploadAsync(stream);
            }

            return blobClient.Uri.AbsoluteUri;

        }

        [FunctionName(nameof(WriteMessagesToSB))]
        [return: ServiceBus("test", Connection = "ServiceBusConnection")]
        public static Message WriteMessagesToSB([ActivityTrigger] IDurableActivityContext writeSbContext, ILogger log)
        {
            //Simulating a long running process
            //Token lifetime aquired by function runtime is 30sec
            //Sleep for 40 second to force a run longer than token runtime
            log.LogInformation("Sleep Start");
            Thread.Sleep(40000);
            log.LogInformation("Sleep End");

            var blobPath = writeSbContext.GetInput<string>();
            //log.LogInformation($"Blob Path:{blobPath}");

            var returnMsg = new Message();
            returnMsg.UserProperties.Add("MessageType", "FromFunc");
            returnMsg.Body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new { path = blobPath }));

            return returnMsg;
        }


        [FunctionName("ProcessSBMessages_DurableClient")]
        public static async void Run(
            [ServiceBusTrigger("test", "testsub", Connection = "ServiceBusConnection")] Message mySbMsg,
            MessageReceiver messageReceiver,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            var msg = JsonConvert.DeserializeObject<NewMessage>(Encoding.UTF8.GetString(mySbMsg.Body));
            msgId = mySbMsg.MessageId;
            lockToken = mySbMsg.SystemProperties.LockToken;
            msgReceiver = messageReceiver;


            string instanceId = await starter.StartNewAsync<object>("ProcessSBMessages", msg);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

        }
    }

    public class NewMessage
    {
        public string Message { get; set; }
    }
}