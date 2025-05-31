using Azure.Storage.Blobs;
using DatafordelerConverter;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.IO.Compression;

// Helper method for memory monitoring
static void LogMemoryUsage(string operation)
{
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
    
    var memoryBefore = GC.GetTotalMemory(false);
    var process = Process.GetCurrentProcess();
    var workingSet = process.WorkingSet64;
    
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Memory - {operation}: GC={memoryBefore / 1024 / 1024:F1}MB, WorkingSet={workingSet / 1024 / 1024:F1}MB");
}

// Start timing
var overallStartTime = DateTime.Now;
var stopwatch = Stopwatch.StartNew();
Console.WriteLine($"[{overallStartTime:HH:mm:ss}] *** DatafordelerConverter Started ***");
LogMemoryUsage("Application Start");

// Build configuration
var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

var storageConnectionString = config["Azure:StorageConnectionString"];
var rawZipsContainerName = config["Azure:RawZipsContainerName"];
var stagingContainerName = config["Azure:StagingContainerName"];
var processedContainerName = config["Azure:ProcessedContainerName"];

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

var zipProcessingEndTime = DateTime.Now;
var zipProcessingDuration = zipProcessingEndTime - overallStartTime;
Console.WriteLine($"[{zipProcessingEndTime:HH:mm:ss}] *** All zip files processed and JSON files uploaded to the staging container ***");
Console.WriteLine($"[{zipProcessingEndTime:HH:mm:ss}] Zip processing duration: {zipProcessingDuration.TotalSeconds:F2}s");

// Ensure the JSON file names are available
if (string.IsNullOrEmpty(darJsonFileName) || string.IsNullOrEmpty(matJsonFileName))
{
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Error: DAR or MAT JSON file names could not be determined.");
    return;
}

// Stream JSON files directly from Azure Blob Storage for processing
var csvProcessingStartTime = DateTime.Now;
Console.WriteLine($"[{csvProcessingStartTime:HH:mm:ss}] *** Streaming JSON files from the staging container for processing ***");

// Get blob clients
var darBlobClient = stagingContainerClient.GetBlobClient(darJsonFileName);
var matBlobClient = stagingContainerClient.GetBlobClient(matJsonFileName);

var lookupStartTime = DateTime.Now;
Console.WriteLine($"[{lookupStartTime:HH:mm:ss}] *** Loading lookup data ***");
LogMemoryUsage("Before Lookup Loading");
using var darStream = await darBlobClient.OpenReadAsync();
var (kommunedelLookup, postnummerLookup) = CommonDataLoader.BuildLookups(darStream);
var lookupEndTime = DateTime.Now;
var lookupDuration = lookupEndTime - lookupStartTime;
LogMemoryUsage("After Lookup Loading");
Console.WriteLine($"[{lookupEndTime:HH:mm:ss}] Lookup data loaded. Duration: {lookupDuration.TotalSeconds:F2}s");

// Generate CSV files in parallel to maximize performance
var parallelProcessingStartTime = DateTime.Now;
Console.WriteLine($"[{parallelProcessingStartTime:HH:mm:ss}] *** Starting Parallel Processing of All CSV Files ***");
LogMemoryUsage("Before Parallel Processing");

// Define parallel tasks for independent processing
var roadNameTask = Task.Run(async () =>
{
    var startTime = DateTime.Now;
    Console.WriteLine($"[{startTime:HH:mm:ss}] *** Processing RoadName (Parallel) ***");
    var roadNameBlobClient = processedContainerClient.GetBlobClient("RoadName.csv");
    using var darStreamRoadName = await darBlobClient.OpenReadAsync();
    using var roadNameStream = await roadNameBlobClient.OpenWriteAsync(overwrite: true);
    RoadName.ExportNavngivenVejToCsv(darStreamRoadName, roadNameStream, kommunedelLookup);
    var endTime = DateTime.Now;
    var duration = endTime - startTime;
    Console.WriteLine($"[{endTime:HH:mm:ss}] RoadName processing completed. Duration: {duration.TotalSeconds:F2}s");
    return duration;
});

var postCodeTask = Task.Run(async () =>
{
    var startTime = DateTime.Now;
    Console.WriteLine($"[{startTime:HH:mm:ss}] *** Processing PostCode (Parallel) ***");
    var postCodeBlobClient = processedContainerClient.GetBlobClient("PostCode.csv");
    using var postCodeStream = await postCodeBlobClient.OpenWriteAsync(overwrite: true);
    PostCode.ExportPostnummerToCsv(postCodeStream, postnummerLookup);
    var endTime = DateTime.Now;
    var duration = endTime - startTime;
    Console.WriteLine($"[{endTime:HH:mm:ss}] PostCode processing completed. Duration: {duration.TotalSeconds:F2}s");
    return duration;
});

var addressAccessTask = Task.Run(async () =>
{
    var startTime = DateTime.Now;
    Console.WriteLine($"[{startTime:HH:mm:ss}] *** Processing AddressAccess (Parallel) ***");
    var addressAccessBlobClient = processedContainerClient.GetBlobClient("AddressAccess.csv");
    using var darStreamAddressAccess = await darBlobClient.OpenReadAsync();
    using var matStreamAddressAccess = await matBlobClient.OpenReadAsync();
    using var addressAccessStream = await addressAccessBlobClient.OpenWriteAsync(overwrite: true);
    await AddressAccess.ExportHusnummerToCsvAsync(darStreamAddressAccess, matStreamAddressAccess, addressAccessStream, kommunedelLookup, postnummerLookup);
    var endTime = DateTime.Now;
    var duration = endTime - startTime;
    Console.WriteLine($"[{endTime:HH:mm:ss}] AddressAccess processing completed. Duration: {duration.TotalSeconds:F2}s");
    return duration;
});

var addressSpecificTask = Task.Run(async () =>
{
    var startTime = DateTime.Now;
    Console.WriteLine($"[{startTime:HH:mm:ss}] *** Processing AddressSpecific (Parallel) ***");
    var addressSpecificBlobClient = processedContainerClient.GetBlobClient("AddressSpecific.csv");
    using var darStreamAddressSpecific = await darBlobClient.OpenReadAsync();
    using var addressSpecificStream = await addressSpecificBlobClient.OpenWriteAsync(overwrite: true);
    AddressSpecificExporter.ExportAddressSpecificToCsv(darStreamAddressSpecific, addressSpecificStream);
    var endTime = DateTime.Now;
    var duration = endTime - startTime;
    Console.WriteLine($"[{endTime:HH:mm:ss}] AddressSpecific processing completed. Duration: {duration.TotalSeconds:F2}s");
    return duration;
});

// Wait for all tasks to complete
Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Waiting for all parallel tasks to complete...");
var taskResults = await Task.WhenAll(roadNameTask, postCodeTask, addressAccessTask, addressSpecificTask);

var parallelProcessingEndTime = DateTime.Now;
var totalParallelDuration = parallelProcessingEndTime - parallelProcessingStartTime;
LogMemoryUsage("After Parallel Processing");

// Report individual and total durations
Console.WriteLine($"[{parallelProcessingEndTime:HH:mm:ss}] *** All Parallel Processing Completed ***");
Console.WriteLine($"[{parallelProcessingEndTime:HH:mm:ss}] Individual task durations:");
Console.WriteLine($"[{parallelProcessingEndTime:HH:mm:ss}]   - RoadName: {taskResults[0].TotalSeconds:F2}s");
Console.WriteLine($"[{parallelProcessingEndTime:HH:mm:ss}]   - PostCode: {taskResults[1].TotalSeconds:F2}s");
Console.WriteLine($"[{parallelProcessingEndTime:HH:mm:ss}]   - AddressAccess: {taskResults[2].TotalSeconds:F2}s");
Console.WriteLine($"[{parallelProcessingEndTime:HH:mm:ss}]   - AddressSpecific: {taskResults[3].TotalSeconds:F2}s");
Console.WriteLine($"[{parallelProcessingEndTime:HH:mm:ss}] Total parallel processing time: {totalParallelDuration.TotalSeconds:F2}s ({totalParallelDuration.TotalMinutes:F2} minutes)");

var completionTime = DateTime.Now;
var totalDuration = completionTime - overallStartTime;
Console.WriteLine($"[{completionTime:HH:mm:ss}] *** CSV files uploaded to the processed container ***");
Console.WriteLine($"[{completionTime:HH:mm:ss}] *** DatafordelerConverter Completed ***");
Console.WriteLine($"[{completionTime:HH:mm:ss}] Total processing time: {totalDuration.TotalSeconds:F2}s ({totalDuration.TotalMinutes:F2} minutes)");

// Stop timing and print elapsed time
stopwatch.Stop();
Console.WriteLine($"\nStopwatch elapsed time: {stopwatch.Elapsed}");
