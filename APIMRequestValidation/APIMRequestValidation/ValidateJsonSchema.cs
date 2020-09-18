using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Extensions.Primitives;
using System.Web.Http;
using NSwag;
using Microsoft.Identity.Client;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json.Linq;

namespace APIMRequestValidation
{
    public static class ValidateJsonSchema
    {
        private static readonly HttpClient Client = new HttpClient();

        [FunctionName("ValidateJsonSchema")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
                log.LogInformation("ValidateJsonSchema Called");

                // Read values passed as headers
                req.Headers.TryGetValue("subscriptionId", out StringValues subscriptionId);
                req.Headers.TryGetValue("resourceGroupId", out StringValues resourceGroupId);
                req.Headers.TryGetValue("apimInstanceId", out StringValues apimInstanceId);
                req.Headers.TryGetValue("apiId", out StringValues apiId);
                req.Headers.TryGetValue("schemaId", out StringValues schemaId);
                req.Headers.TryGetValue("schemaName", out StringValues schemaName);

                // Read JSON to validate
                var jsonBody = await new StreamReader(req.Body).ReadToEndAsync();

                // Obtain accesstoken
                string clientId = Environment.GetEnvironmentVariable("ClientId");
                string clientSecret = Environment.GetEnvironmentVariable("ClientSecret");
                string tenantId = Environment.GetEnvironmentVariable("TenantId");
                string[] scopes = new string[] { $"{Environment.GetEnvironmentVariable("ValidateJson_Audience")}" };
                string url = String.Format("https://login.microsoftonline.com/{0}/oauth2/v2.0/token", tenantId);

                var app = ConfidentialClientApplicationBuilder.Create(clientId)
                    .WithClientSecret(clientSecret)
                    .WithAuthority(new Uri(url))
                    .Build();

                var result = await app.AcquireTokenForClient(scopes).ExecuteAsync();

                var accessToken = result.AccessToken;

                log.LogInformation("Getting Schema from APIM");

                // Get schema from API Management Instance
                HttpRequestMessage schemaRequest = new HttpRequestMessage
                {
                    RequestUri = new Uri(@$"https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupId}" +
                                $@"/providers/Microsoft.ApiManagement/service/{apimInstanceId}/apis/{apiId}/schemas/{schemaId}?api-version=2019-01-01"),
                    Method = HttpMethod.Get
                };

                log.LogInformation($"Fetch schema from URL: {schemaRequest.RequestUri.AbsoluteUri}");

                Client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                var schemaResponse = await Client.SendAsync(schemaRequest);
                var schemaResponseContent = await schemaResponse.Content.ReadAsStringAsync();

                log.LogInformation($"Schema response : {schemaResponseContent}");

                // Convert response into OpenApi document
                var schemaResponseJSON = JsonConvert.DeserializeObject<JObject>(schemaResponseContent);
                var componentToken = schemaResponseJSON.SelectToken("properties.document");
                componentToken["openapi"] = "3.0.1";
                var openApiSpec = JsonConvert.SerializeObject(componentToken);

                // Get schema to validate from OpenApi document
                var document = OpenApiDocument.FromJsonAsync(openApiSpec);
                var sch = document.Result.Components.Schemas[schemaName];

                // Validate schema against JSON supplied
                var test = sch.Validate(jsonBody);

                // Check if there are validation errors
                if (test.Count > 0)
                {
                    log.LogInformation("JSON Validation Failed");
                    var validationErrors = JsonConvert.SerializeObject(test, new Newtonsoft.Json.Converters.StringEnumConverter());
                    log.LogInformation($"Validation Errors : {validationErrors}");

                    // Return validation errors to the caller
                    return new ObjectResult(validationErrors) { StatusCode = StatusCodes.Status422UnprocessableEntity };
                }

                log.LogInformation("JSON Validation Successful");
                return new OkResult();
            }
            catch (Exception ex)
            {
                log.LogInformation($"Exception occurred : {ex.Message}");
                return new InternalServerErrorResult();
            }
        }
    }
}
