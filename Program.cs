using DatafordelerConverter;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

// Build configuration
var config = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

var darJsonFilepath = config["Paths:DarJson"];
var roadNameCsvFilepath = config["Paths:RoadNameCsv"];
var postCodeCsvFilepath = config["Paths:PostCodeCsv"];
var addressAccessCsvFilepath = config["Paths:AddressAccessCsv"];

Console.WriteLine("*** Loading common data ***");
var kommunedelLookup = CommonDataLoader.BuildNavngivenVejKommunedelLookup(darJsonFilepath);
var postnummerLookup = CommonDataLoader.BuildPostnummerLookup(darJsonFilepath);

Console.WriteLine("");
Console.WriteLine("*** Processing RoadName ***");
RoadName.ExportNavngivenVejToCsv(darJsonFilepath, roadNameCsvFilepath, kommunedelLookup);

Console.WriteLine();
Console.WriteLine("*** Processing PostCode ***");
PostCode.ExportPostnummerToCsv(darJsonFilepath, postCodeCsvFilepath);

Console.WriteLine();
Console.WriteLine("*** Processing AddressAccess ***");
AddressAccess.ExportHusnummerToCsv(darJsonFilepath, addressAccessCsvFilepath, kommunedelLookup, postnummerLookup);