using System.Text;
using System.Text.Json;

namespace DatafordelerConverter;

public static class RoadName
{
    public static void ExportNavngivenVejToCsv(Stream jsonStream, string csvFile, Dictionary<string, (string kommune, string vejkode)> kommunedelLookup)
    {
        Console.Write("\rSearching...");
        using var sw = new StreamWriter(csvFile, false, Encoding.UTF8, 65536);
        sw.WriteLine("MunicipalityCode;StreetId;Name");

        var options = new JsonReaderOptions { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip };
        byte[] buffer;
        using (var ms = new MemoryStream())
        {
            jsonStream.CopyTo(ms);
            buffer = ms.ToArray();
        }
        var reader = new Utf8JsonReader(buffer, options);
        var navngivenVejCounter = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.PropertyName && reader.GetString() == "NavngivenVejList")
            {
                Console.Write("\rLoading NavngivenVejList");
                reader.Read(); // StartArray
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        string id_lokalId = null, vejnavn = null;
                        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                        {
                            if (reader.TokenType == JsonTokenType.PropertyName)
                            {
                                var prop = reader.GetString();
                                reader.Read();
                                if (prop == "id_lokalId" && reader.TokenType == JsonTokenType.String)
                                    id_lokalId = reader.GetString();
                                else if (prop == "vejnavn" && reader.TokenType == JsonTokenType.String)
                                    vejnavn = reader.GetString();
                                else
                                    reader.Skip();
                            }
                        }
                        if (!string.IsNullOrEmpty(id_lokalId) && !string.IsNullOrEmpty(vejnavn) && kommunedelLookup.TryGetValue(id_lokalId, out var kommunedel))
                        {
                            navngivenVejCounter++;
                            sw.WriteLine($"{kommunedel.kommune};{kommunedel.vejkode};{vejnavn}");
                        }
                    }
                }
                break;
            }
        }

        sw.Flush();
        Console.WriteLine($"\rDone writing RoadName.csv - {navngivenVejCounter} lines.");
    }
}