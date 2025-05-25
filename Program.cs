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

RoadName.ExportNavngivenVejToCsv(darJsonFilepath, roadNameCsvFilepath);
PostCode.ExportPostnummerToCsv(darJsonFilepath, postCodeCsvFilepath);
AddressAccess.ExportHusnummerToCsv(darJsonFilepath, addressAccessCsvFilepath);

//ExportNavngivenVejToCsv(darJsonFilepath, roadNameCsvFilepath);
//ExportPostnummerListToCsv(darJsonFilepath, postCodeCsvFilepath);
return;
void ExportNavngivenVejToCsv(string jsonFile, string csvFile)
{
    // 1. Build lookup: navngivenVej => (kommune, vejkode)
    var kommunedelLookup = new Dictionary<string, (string kommune, string vejkode)>();

    using (var sr = new StreamReader(jsonFile))
    using (var reader = new JsonTextReader(sr))
    {
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "NavngivenVejKommunedelList")
            {
                var navngivenVejKommunedelListCounter = 0;
                reader.Read(); // StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;

                // Load the array into memory for fast processing
                var array = JArray.Load(reader);
                foreach (var item in array)
                {
                    var navngivenVej = (string?)item["navngivenVej"];
                    var kommune = (string?)item["kommune"];
                    var vejkode = (string?)item["vejkode"];
                    if (!string.IsNullOrEmpty(navngivenVej) && kommune != null && vejkode != null)
                    {
                        if (!kommunedelLookup.ContainsKey(navngivenVej))
                        {
                            navngivenVejKommunedelListCounter++;
                            kommunedelLookup[navngivenVej] = (kommune, vejkode);
                            Console.Write($"\rnavngivenVejKommunedelListCounter: {navngivenVejKommunedelListCounter}");
                        }
                    }
                }
                Console.WriteLine($"\rnavngivenVejKommunedelListCounter: {navngivenVejKommunedelListCounter}");
                break; // Done with NavngivenVejKommunedelList
            }
        }
    }

    // 2. Stream NavngivenVejList and write CSV
    using (var sr = new StreamReader(jsonFile))
    using (var reader = new JsonTextReader(sr))
    using (var sw = new StreamWriter(csvFile, false, System.Text.Encoding.UTF8))
    {
        sw.WriteLine("MunicipalityCode;StreetId;Name");

        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "NavngivenVejList")
            {
                var navngivenVejCounter = 0;
                reader.Read(); // StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;

                while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                {
                    if (reader.TokenType == JsonToken.StartObject)
                    {
                        string id_lokalId = null, vejnavn = null;
                        while (reader.Read() && reader.TokenType != JsonToken.EndObject)
                        {
                            if (reader.TokenType == JsonToken.PropertyName)
                            {
                                var prop = (string)reader.Value;
                                reader.Read();
                                switch (prop)
                                {
                                    case "id_lokalId":
                                        id_lokalId = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    case "vejnavn":
                                        vejnavn = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    default:
                                        reader.Skip();
                                        break;
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(id_lokalId) && !string.IsNullOrEmpty(vejnavn))
                        {
                            if (kommunedelLookup.TryGetValue(id_lokalId, out var kommunedel))
                            {
                                navngivenVejCounter++;
                                sw.WriteLine($"{kommunedel.kommune};{kommunedel.vejkode};{vejnavn}");
                            }
                        }
                    }
                    Console.Write($"\rNavngivenVejCounter: {navngivenVejCounter}");
                }
                Console.WriteLine($"\rNavngivenVejCounter: {navngivenVejCounter}");
                break; // Done with NavngivenVejList
            }
        }
        sw.Flush();
        Console.WriteLine("\nDone writing RoadName.csv");
    }
}

void ExportPostnummerListToCsv(string jsonFile, string csvFile)
{
    using var sr = new StreamReader(jsonFile);
    using var reader = new JsonTextReader(sr);
    using var sw = new StreamWriter(csvFile, false, System.Text.Encoding.UTF8);
    var postnummerCounter = 0;
    sw.WriteLine("PostalCode;;PostalDistrictName");

    while (reader.Read())
    {
        if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "PostnummerList")
        {
            reader.Read(); // StartArray
            if (reader.TokenType != JsonToken.StartArray)
                break;

            while (reader.Read() && reader.TokenType != JsonToken.EndArray)
            {
                if (reader.TokenType == JsonToken.StartObject)
                {
                    string postnr = null, navn = null;
                    while (reader.Read() && reader.TokenType != JsonToken.EndObject)
                    {
                        if (reader.TokenType == JsonToken.PropertyName)
                        {
                            var prop = (string)reader.Value;
                            reader.Read();
                            switch (prop)
                            {
                                case "postnr":
                                    postnr = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                    break;
                                case "navn":
                                    navn = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                    break;
                                default:
                                    reader.Skip();
                                    break;
                            }
                        }
                    }
                    if (!string.IsNullOrEmpty(postnr) && !string.IsNullOrEmpty(navn))
                    {
                        postnummerCounter++;
                        sw.WriteLine($"{postnr};;{navn}");
                    }
                }
                if (postnummerCounter % 1000 == 0)
                    Console.Write($"\rPostnummerCounter: {postnummerCounter}");
            }
            break; // Done with PostnummerList
        }
    }
    sw.Flush();
    Console.WriteLine($"\nDone writing PostCode.csv - {postnummerCounter}");
}
