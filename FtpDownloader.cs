using Azure.Storage.Blobs;
using FluentFTP;
using System.Globalization;
using System.Text.RegularExpressions;

namespace DatafordelerConverter;

public class FtpDownloader
{
    private readonly string _host;
    private readonly int _port;
    private readonly string _username;
    private readonly string _password;
    private readonly string _remoteDirectory;
    private readonly bool _useSSL;

    public FtpDownloader(string host, int port, string username, string password, string remoteDirectory, bool useSSL = false)
    {
        _host = host;
        _port = port;
        _username = username;
        _password = password;
        _remoteDirectory = remoteDirectory;
        _useSSL = useSSL;
    }

    /// <summary>
    /// Downloads the newest files matching the specified prefixes from FTP and uploads them directly to Azure Blob Storage
    /// </summary>
    /// <param name="blobContainerClient">Azure Blob Storage container client</param>
    /// <param name="darPrefix">Prefix for DAR files (e.g., "DAR_AKTUELT_TOTAL_01_")</param>
    /// <param name="matPrefix">Prefix for MAT files (e.g., "MAT_AKTUELT_TOTAL_01_")</param>
    /// <returns>Tuple containing the names of the downloaded DAR and MAT files</returns>
    public async Task<(string? darFileName, string? matFileName)> DownloadLatestFilesToBlobAsync(
        BlobContainerClient blobContainerClient, 
        string darPrefix = "DAR_AKTUELT_TOTAL_01_", 
        string matPrefix = "MAT_AKTUELT_TOTAL_01_")
    {
        using var ftpClient = new FtpClient(_host, _username, _password, _port);
        
        if (_useSSL)
        {
            ftpClient.Config.EncryptionMode = FtpEncryptionMode.Explicit;
            ftpClient.Config.ValidateAnyCertificate = true;
        }

        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Connecting to FTP server: {_host}:{_port}");
        ftpClient.Connect();

        try
        {
            // Change to the remote directory if specified
            if (!string.IsNullOrEmpty(_remoteDirectory))
            {
                ftpClient.SetWorkingDirectory(_remoteDirectory);
            }

            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Listing files in FTP directory...");
            var ftpFiles = ftpClient.GetListing();

            // Find the latest DAR file
            var darFile = FindLatestFile(ftpFiles, darPrefix);
            var matFile = FindLatestFile(ftpFiles, matPrefix);

            if (darFile == null)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Warning: No DAR file found with prefix '{darPrefix}'");
            }

            if (matFile == null)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Warning: No MAT file found with prefix '{matPrefix}'");
            }

            string? darFileName = null;
            string? matFileName = null;

            // Download DAR file if found
            if (darFile != null)
            {
                darFileName = await DownloadFileToBlob(ftpClient, blobContainerClient, darFile);
            }

            // Download MAT file if found
            if (matFile != null)
            {
                matFileName = await DownloadFileToBlob(ftpClient, blobContainerClient, matFile);
            }

            return (darFileName, matFileName);
        }
        finally
        {
            ftpClient.Disconnect();
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Disconnected from FTP server");
        }
    }

    /// <summary>
    /// Finds the latest file with the specified prefix based on timestamp in filename
    /// </summary>
    private static FtpListItem? FindLatestFile(FtpListItem[] files, string prefix)
    {
        var matchingFiles = files
            .Where(f => f.Type == FtpObjectType.File && f.Name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
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
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Found latest file: {latestFile.File.Name} (timestamp: {latestFile.Timestamp:yyyyMMddHHmmss})");
        
        return latestFile.File;
    }

    /// <summary>
    /// Extracts timestamp from filename in format: PREFIX_YYYYMMDDHHMMSS.ext
    /// </summary>
    private static DateTime? ExtractTimestampFromFilename(string filename, string prefix)
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

    /// <summary>
    /// Downloads a file from FTP and streams it directly to Azure Blob Storage
    /// </summary>
    private static async Task<string> DownloadFileToBlob(FtpClient ftpClient, BlobContainerClient blobContainerClient, FtpListItem ftpFile)
    {
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Downloading {ftpFile.Name} from FTP and uploading to blob storage...");

        var blobClient = blobContainerClient.GetBlobClient(ftpFile.Name);

        // Check if the file already exists in blob storage
        if (await blobClient.ExistsAsync())
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] File {ftpFile.Name} already exists in blob storage, skipping download");
            return ftpFile.Name;
        }

        // Stream the file directly from FTP to Azure Blob Storage
        using var ftpStream = ftpClient.OpenRead(ftpFile.FullName);
        
        // Upload to blob storage with progress tracking
        var uploadOptions = new Azure.Storage.Blobs.Models.BlobUploadOptions
        {
            TransferOptions = new Azure.Storage.StorageTransferOptions
            {
                // Set chunk size for better performance with large files
                InitialTransferSize = 4 * 1024 * 1024, // 4MB
                MaximumTransferSize = 4 * 1024 * 1024   // 4MB
            }
        };

        await blobClient.UploadAsync(ftpStream, uploadOptions);

        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Successfully uploaded {ftpFile.Name} to blob storage (size: {ftpFile.Size:N0} bytes)");
        
        return ftpFile.Name;
    }
}
