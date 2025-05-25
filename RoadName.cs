using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DatafordelerConverter;

public static class RoadName
{
    /// <summary>
    /// Orchestrates the export of NavngivenVej data to CSV.
    /// </summary>
    public static void ExportNavngivenVejToCsv(string jsonFile, string csvFile)
    {
        var kommunedelLookup = BuildNavngivenVejKommunedelLookup(jsonFile);
        ExportNavngivenVejCsvWithLookup(jsonFile, csvFile, kommunedelLookup);
    }

    /// <summary>
    /// Loads NavngivenVejKommunedelList into a lookup dictionary.
    /// </summary>
    private static Dictionary<string, (string kommune, string vejkode)> BuildNavngivenVejKommunedelLookup(string jsonFile)
    {
        var kommunedelLookup = new Dictionary<string, (string kommune, string vejkode)>();
        var counter = 0;
        using var sr = new StreamReader(jsonFile);
        using var reader = new JsonTextReader(sr);
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "NavngivenVejKommunedelList")
            {
                    
                reader.Read(); // StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;

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
                            counter++;
                            kommunedelLookup[navngivenVej] = (kommune, vejkode);
                            Console.Write($"\rnavngivenVejKommunedelListCounter: {counter}");
                        }
                    }
                }
                break;
            }
        }
        Console.WriteLine($"\rDone loading navngivenVejKommunedelList - {counter} items.");
        return kommunedelLookup;
    }

    /// <summary>
    /// Streams NavngivenVejList and writes the CSV using the lookup.
    /// </summary>
    private static void ExportNavngivenVejCsvWithLookup(
        string jsonFile,
        string csvFile,
        Dictionary<string, (string kommune, string vejkode)> kommunedelLookup)
    {
        using var sr = new StreamReader(jsonFile);
        using var reader = new JsonTextReader(sr);
        using var sw = new StreamWriter(csvFile, false, System.Text.Encoding.UTF8);
        sw.WriteLine("MunicipalityCode;StreetId;Name");
        var navngivenVejCounter = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "NavngivenVejList")
            {
                    
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
                    Console.Write($"\rWriting to RoadName.csv: {navngivenVejCounter}");
                }
                break;
            }
        }
        sw.Flush();
        Console.WriteLine($"\rDone writing RoadName.csv - {navngivenVejCounter} lines.");
    }
}