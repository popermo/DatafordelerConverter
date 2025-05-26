using DatafordelerConverter;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

// Start timing
var stopwatch = Stopwatch.StartNew();

// Build configuration
var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

var darJsonFilepath = config["Paths:DarJson"];
var matJsonFilepath = config["Paths:MatJson"];
var roadNameCsvFilepath = config["Paths:RoadNameCsv"];
var postCodeCsvFilepath = config["Paths:PostCodeCsv"];
var addressAccessCsvFilepath = config["Paths:AddressAccessCsv"];
var addressSpecificCsvFilepath = config["Paths:AddressSpecificCsv"];

Console.WriteLine("*** Loading common data ***");
var kommunedelLookup = CommonDataLoader.BuildNavngivenVejKommunedelLookup(darJsonFilepath);
var postnummerLookup = CommonDataLoader.BuildPostnummerLookup(darJsonFilepath);

Console.WriteLine("");
Console.WriteLine("*** Processing RoadName ***");
RoadName.ExportNavngivenVejToCsv(darJsonFilepath, roadNameCsvFilepath, kommunedelLookup);

Console.WriteLine();
Console.WriteLine("*** Processing PostCode ***");
PostCode.ExportPostnummerToCsv(postCodeCsvFilepath, postnummerLookup);

Console.WriteLine();
Console.WriteLine("*** Processing AddressAccess ***");
AddressAccess.ExportHusnummerToCsv(darJsonFilepath, matJsonFilepath, addressAccessCsvFilepath, kommunedelLookup, postnummerLookup);

Console.WriteLine();
Console.WriteLine("*** Processing AddressSpecific ***");
AddressSpecificExporter.ExportAddressSpecificToCsv(darJsonFilepath, addressSpecificCsvFilepath);

// Stop timing and print elapsed time
stopwatch.Stop();
Console.WriteLine();
Console.WriteLine($"\nTotal elapsed time: {stopwatch.Elapsed}");
