using System.Text;
using Newtonsoft.Json;

namespace DatafordelerConverter;

public static class RoadName
{
    public static void ExportNavngivenVejToCsv(Stream jsonStream, Stream outputStream, Dictionary<string, (string kommune, string vejkode)> kommunedelLookup)
    {
        Console.Write("\rSearching...");
        using var sw = new StreamWriter(outputStream, Encoding.UTF8, 65536, leaveOpen: true);
        sw.WriteLine("MunicipalityCode;StreetId;Name");

        using var sr = new StreamReader(jsonStream);
        using var reader = new JsonTextReader(sr);
        var navngivenVejCounter = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "NavngivenVejList")
            {
                Console.Write("\rLoading NavngivenVejList");
                reader.Read(); // Move to StartArray
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
                                
                                if (prop == "id_lokalId" && reader.TokenType == JsonToken.String)
                                    id_lokalId = (string)reader.Value;
                                else if (prop == "vejnavn" && reader.TokenType == JsonToken.String)
                                    vejnavn = (string)reader.Value;
                                else
                                    reader.Skip();
                            }
                        }
                        
                        if (!string.IsNullOrEmpty(id_lokalId) && !string.IsNullOrEmpty(vejnavn) && 
                            kommunedelLookup.TryGetValue(id_lokalId, out var kommunedel))
                        {
                            navngivenVejCounter++;
                            sw.WriteLine($"{kommunedel.kommune};{kommunedel.vejkode};{vejnavn}");
                            
                            if (navngivenVejCounter % 1000 == 0)
                                Console.Write($"\rProcessing NavngivenVejList: {navngivenVejCounter}");
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
