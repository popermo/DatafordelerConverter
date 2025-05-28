using Azure.Storage.Blobs;
using DatafordelerConverter;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.IO.Compression;

// Start timing
var stopwatch = Stopwatch.StartNew();

// Build configuration
var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

var storageConnectionString = config["Azure:StorageConnectionString"];
var rawZipsContainerName = config["Azure:RawZipsContainerName"];
var stagingContainerName = config["Azure:StagingContainerName"];
var processedContainerName = config["Azure:ProcessedContainerName"];

var roadNameCsvFilepath = config["Paths:RoadNameCsv"];
var postCodeCsvFilepath = config["Paths:PostCodeCsv"];
var addressAccessCsvFilepath = config["Paths:AddressAccessCsv"];
var addressSpecificCsvFilepath = config["Paths:AddressSpecificCsv"];

var blobServiceClient = new BlobServiceClient(storageConnectionString);
var rawZipsContainerClient = blobServiceClient.GetBlobContainerClient(rawZipsContainerName);
var stagingContainerClient = blobServiceClient.GetBlobContainerClient(stagingContainerName);
var processedContainerClient = blobServiceClient.GetBlobContainerClient(processedContainerName);

string? darJsonFileName = null;
string? matJsonFileName = null;

// Process all zip files in the raw-zips container
await foreach (var blobItem in rawZipsContainerClient.GetBlobsAsync())
{
    if (blobItem.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
    {
        Console.WriteLine($"Processing zip file: {blobItem.Name}");

        // Check if a JSON file with the same name already exists in the staging container
        var jsonFileName = Path.ChangeExtension(blobItem.Name, ".json");
        var jsonBlobClient = stagingContainerClient.GetBlobClient(jsonFileName);

        if (await jsonBlobClient.ExistsAsync())
        {
            Console.WriteLine($"Skipping zip file: {blobItem.Name} because {jsonFileName} already exists in the staging container.");

            // Set the JSON file name if it already exists
            if (jsonFileName.StartsWith("DAR_", StringComparison.OrdinalIgnoreCase))
            {
                darJsonFileName = jsonFileName;
            }
            else if (jsonFileName.StartsWith("MAT_", StringComparison.OrdinalIgnoreCase))
            {
                matJsonFileName = jsonFileName;
            }

            continue;
        }

        // Download the zip file to a temporary file
        var zipBlobClient = rawZipsContainerClient.GetBlobClient(blobItem.Name);
        var tempZipFilePath = Path.GetTempFileName();
        Console.WriteLine($"Downloading zip file to temporary file: {tempZipFilePath}");
        await using (var fileStream = File.OpenWrite(tempZipFilePath))
        {
            await zipBlobClient.DownloadToAsync(fileStream);
        }

        // Extract and upload JSON files to the staging container
        using (var archive = ZipFile.OpenRead(tempZipFilePath))
        {
            foreach (var entry in archive.Entries)
            {
                if (entry.FullName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                {
                    // Skip files containing "Metadata" in their names
                    if (entry.FullName.Contains("Metadata", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine($"Skipping file: {entry.FullName} (contains 'Metadata')");
                        continue;
                    }

                    Console.WriteLine($"Checking if {entry.FullName} already exists in the staging container...");
                    var entryBlobClient = stagingContainerClient.GetBlobClient(entry.FullName);

                    // Check if the file already exists
                    if (await entryBlobClient.ExistsAsync())
                    {
                        Console.WriteLine($"Skipping upload: {entry.FullName} already exists in the staging container.");

                        // Set the JSON file name if it already exists
                        if (entry.FullName.StartsWith("DAR_", StringComparison.OrdinalIgnoreCase))
                        {
                            darJsonFileName = entry.FullName;
                        }
                        else if (entry.FullName.StartsWith("MAT_", StringComparison.OrdinalIgnoreCase))
                        {
                            matJsonFileName = entry.FullName;
                        }

                        continue;
                    }

                    Console.WriteLine($"Uploading {entry.FullName} to the staging container...");

                    // Save the file name for later use
                    if (entry.FullName.StartsWith("DAR_", StringComparison.OrdinalIgnoreCase))
                    {
                        darJsonFileName = entry.FullName;
                    }
                    else if (entry.FullName.StartsWith("MAT_", StringComparison.OrdinalIgnoreCase))
                    {
                        matJsonFileName = entry.FullName;
                    }

                    // Stream the JSON file directly to Azure Blob Storage
                    using var entryStream = entry.Open();
                    await entryBlobClient.UploadAsync(entryStream, overwrite: true);
                }
            }
        }

        // Delete the temporary zip file
        Console.WriteLine($"Deleting temporary file: {tempZipFilePath}");
        File.Delete(tempZipFilePath);
    }
}

Console.WriteLine("*** All zip files processed and JSON files uploaded to the staging container ***");

// Stop timing and print elapsed time
stopwatch.Stop();
Console.WriteLine($"\nTotal elapsed time: {stopwatch.Elapsed}");

// Ensure the JSON file names are available
if (string.IsNullOrEmpty(darJsonFileName) || string.IsNullOrEmpty(matJsonFileName))
{
    Console.WriteLine("Error: DAR or MAT JSON file names could not be determined.");
    return;
}

// Stream JSON files directly from Azure Blob Storage for processing
Console.WriteLine("*** Streaming JSON files from the staging container for processing ***");

// Stream DAR JSON file
var darBlobClient = stagingContainerClient.GetBlobClient(darJsonFileName);
// Stream MAT JSON file
var matBlobClient = stagingContainerClient.GetBlobClient(matJsonFileName);

Console.WriteLine("*** Loading lookup data ***");
using var darStream = await darBlobClient.OpenReadAsync();
var (kommunedelLookup, postnummerLookup) = CommonDataLoader.BuildLookups(darStream);

// Generate CSV files
Console.WriteLine("*** Processing RoadName ***");
using var darStreamRoadName = await darBlobClient.OpenReadAsync();
RoadName.ExportNavngivenVejToCsv(darStreamRoadName, roadNameCsvFilepath, kommunedelLookup);

Console.WriteLine("*** Processing PostCode ***");
PostCode.ExportPostnummerToCsv(postCodeCsvFilepath, postnummerLookup);

Console.WriteLine("*** Processing AddressAccess ***");
using var darStreamAddressAccess = await darBlobClient.OpenReadAsync();
using var matStreamAddressAccess = await matBlobClient.OpenReadAsync();
AddressAccess.ExportHusnummerToCsv(darStreamAddressAccess, matStreamAddressAccess, addressAccessCsvFilepath, kommunedelLookup, postnummerLookup);

Console.WriteLine("*** Processing AddressSpecific ***");
using var darStreamAddressSpecific = await darBlobClient.OpenReadAsync();
AddressSpecificExporter.ExportAddressSpecificToCsv(darStreamAddressSpecific, addressSpecificCsvFilepath);

// Upload the generated CSV files to the processed container
UploadCsvToProcessedContainer(processedContainerClient, roadNameCsvFilepath);
UploadCsvToProcessedContainer(processedContainerClient, postCodeCsvFilepath);
UploadCsvToProcessedContainer(processedContainerClient, addressAccessCsvFilepath);
UploadCsvToProcessedContainer(processedContainerClient, addressSpecificCsvFilepath);

Console.WriteLine("*** CSV files uploaded to the processed container ***");

// Stop timing and print elapsed time
stopwatch.Stop();
Console.WriteLine($"\nTotal elapsed time: {stopwatch.Elapsed}");

/// <summary>
/// Uploads a CSV file to the processed container in Azure Blob Storage.
/// </summary>
/// <param name="containerClient"></param>
/// <param name="csvFilePath"></param>
void UploadCsvToProcessedContainer(BlobContainerClient containerClient, string csvFilePath)
{
    var blobName = Path.GetFileName(csvFilePath);
    var blobClient = containerClient.GetBlobClient(blobName);

    Console.WriteLine($"Uploading {blobName} to the processed container...");
    using var fileStream = File.OpenRead(csvFilePath);
    blobClient.Upload(fileStream, overwrite: true);
}
