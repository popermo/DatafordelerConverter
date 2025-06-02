using Azure.Storage.Blobs;
using DatafordelerConverter;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.IO.Compression;
using System.Globalization;

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

// Helper method to extract timestamp from filename
static DateTime? ExtractTimestampFromFilename(string filename, string prefix)
{
    try
    {
        // Remove the prefix and file extension
        var withoutPrefix = filename.Substring(prefix.Length);
        var timestampPart = Path.GetFileNameWithoutExtension(withoutPrefix);

        // The timestamp should be exactly 14 characters (YYYYMMDDHHMMSS)
        if (timestampPart.Length == 14 && timestampPart.All(char.IsDigit))
        {
            if (DateTime.TryParseExact(timestampPart, "yyyyMMddHHmmss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var timestamp))
            {
                return timestamp;
            }
        }

        return null;
    }
    catch
    {
        return null;
    }
}

// Helper method to find the latest file with specified prefix
static string? FindLatestZipFile(IEnumerable<Azure.Storage.Blobs.Models.BlobItem> blobs, string prefix)
{
    var matchingFiles = blobs
        .Where(b => b.Name.EndsWith(".zip", StringComparison.OrdinalIgnoreCase) && 
                   b.Name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        .ToList();

    if (!matchingFiles.Any())
    {
        return null;
    }

    // Extract timestamp from filename and find the latest
    var filesWithTimestamps = matchingFiles
        .Select(f => new
        {
            File = f,
            Timestamp = ExtractTimestampFromFilename(f.Name, prefix)
        })
        .Where(x => x.Timestamp.HasValue)
        .OrderByDescending(x => x.Timestamp.Value)
        .ToList();

    if (!filesWithTimestamps.Any())
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Warning: Found files with prefix '{prefix}' but could not parse timestamps");
        return null;
    }

    var latestFile = filesWithTimestamps.First();
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Found latest zip file: {latestFile.File.Name} (timestamp: {latestFile.Timestamp!.Value:yyyyMMddHHmmss})");
    
    return latestFile.File.Name;
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

// FTP configuration
var ftpHost = config["FTP:Host"];
var ftpPort = int.Parse(config["FTP:Port"] ?? "21");
var ftpUsername = config["FTP:Username"];
var ftpPassword = config["FTP:Password"];
var ftpRemoteDirectory = config["FTP:RemoteDirectory"];
var ftpUseSSL = bool.Parse(config["FTP:UseSSL"] ?? "false");

var blobServiceClient = new BlobServiceClient(storageConnectionString);
var rawZipsContainerClient = blobServiceClient.GetBlobContainerClient(rawZipsContainerName);
var stagingContainerClient = blobServiceClient.GetBlobContainerClient(stagingContainerName);
var processedContainerClient = blobServiceClient.GetBlobContainerClient(processedContainerName);

// Download latest files from FTP server
var ftpDownloadStartTime = DateTime.Now;
Console.WriteLine($"[{ftpDownloadStartTime:HH:mm:ss}] *** Starting FTP Download ***");
LogMemoryUsage("Before FTP Download");

var ftpDownloader = new FtpDownloader(ftpHost!, ftpPort, ftpUsername!, ftpPassword!, ftpRemoteDirectory!, ftpUseSSL);
var (downloadedDarFile, downloadedMatFile) = await ftpDownloader.DownloadLatestFilesToBlobAsync(rawZipsContainerClient);

var ftpDownloadEndTime = DateTime.Now;
var ftpDownloadDuration = ftpDownloadEndTime - ftpDownloadStartTime;
LogMemoryUsage("After FTP Download");
Console.WriteLine($"[{ftpDownloadEndTime:HH:mm:ss}] *** FTP Download Completed ***");
Console.WriteLine($"[{ftpDownloadEndTime:HH:mm:ss}] FTP download duration: {ftpDownloadDuration.TotalSeconds:F2}s");

if (downloadedDarFile != null)
{
    Console.WriteLine($"[{ftpDownloadEndTime:HH:mm:ss}] Downloaded DAR file: {downloadedDarFile}");
}

if (downloadedMatFile != null)
{
    Console.WriteLine($"[{ftpDownloadEndTime:HH:mm:ss}] Downloaded MAT file: {downloadedMatFile}");
}

string? darJsonFileName = null;
string? matJsonFileName = null;

// Find the latest DAR and MAT zip files in the storage account
Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] *** Finding latest zip files in storage account ***");
var allBlobs = new List<Azure.Storage.Blobs.Models.BlobItem>();
await foreach (var blobItem in rawZipsContainerClient.GetBlobsAsync())
{
    allBlobs.Add(blobItem);
}

var latestDarZip = FindLatestZipFile(allBlobs, "DAR_AKTUELT_TOTAL_01_");
var latestMatZip = FindLatestZipFile(allBlobs, "MAT_AKTUELT_TOTAL_01_");

if (latestDarZip == null)
{
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Error: No DAR zip file found in storage account");
    return;
}

if (latestMatZip == null)
{
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Error: No MAT zip file found in storage account");
    return;
}

Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Latest DAR zip: {latestDarZip}");
Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Latest MAT zip: {latestMatZip}");

// Process only the latest zip files
var zipFilesToProcess = new[] { latestDarZip, latestMatZip };

foreach (var zipFileName in zipFilesToProcess)
{
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Processing zip file: {zipFileName}");

    // Check if a JSON file with the same name already exists in the staging container
    var jsonFileName = Path.ChangeExtension(zipFileName, ".json");
    var jsonBlobClient = stagingContainerClient.GetBlobClient(jsonFileName);

    if (await jsonBlobClient.ExistsAsync())
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Skipping zip file: {zipFileName} because {jsonFileName} already exists in the staging container.");

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
    var zipBlobClient = rawZipsContainerClient.GetBlobClient(zipFileName);
    var tempZipFilePath = Path.GetTempFileName();
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Downloading zip file to temporary file: {tempZipFilePath}");
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
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Skipping file: {entry.FullName} (contains 'Metadata')");
                    continue;
                }

                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Checking if {entry.FullName} already exists in the staging container...");
                var entryBlobClient = stagingContainerClient.GetBlobClient(entry.FullName);

                // Check if the file already exists
                if (await entryBlobClient.ExistsAsync())
                {
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Skipping upload: {entry.FullName} already exists in the staging container.");

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

                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Uploading {entry.FullName} to the staging container...");

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
    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Deleting temporary file: {tempZipFilePath}");
    File.Delete(tempZipFilePath);
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
