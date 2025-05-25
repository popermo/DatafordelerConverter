using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DatafordelerConverter;

public static class RoadName
{
    /// <summary>
    /// Orchestrates the export of NavngivenVej data to CSV.
    /// </summary>
    public static void ExportNavngivenVejToCsv(string jsonFile, string csvFile, Dictionary<string, (string kommune, string vejkode)> kommunedelLookup)
    {
        ExportNavngivenVejCsvWithLookup(jsonFile, csvFile, kommunedelLookup);
    }

    /// <summary>
    /// Streams NavngivenVejList and writes the CSV using the lookup.
    /// </summary>
    private static void ExportNavngivenVejCsvWithLookup(
        string jsonFile,
        string csvFile,
        Dictionary<string, (string kommune, string vejkode)> kommunedelLookup)
    {
        Console.Write("\rSearching...");
        using var sr = new StreamReader(jsonFile);
        using var reader = new JsonTextReader(sr);
        using var sw = new StreamWriter(csvFile, false, System.Text.Encoding.UTF8);
        sw.WriteLine("MunicipalityCode;StreetId;Name");
        var navngivenVejCounter = 0;

        while (reader.Read())
        {
            
            if (reader.TokenType != JsonToken.PropertyName || (string)reader.Value != "NavngivenVejList") continue;
            Console.Write("\rLoading NavngivenVejList");
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
            }
            break;
        }
        sw.Flush();
        Console.WriteLine($"\rDone writing RoadName.csv - {navngivenVejCounter} lines.");
    }
}