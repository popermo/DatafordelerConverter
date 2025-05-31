using System.Text;
using Newtonsoft.Json;

namespace DatafordelerConverter;

public static class AddressSpecificExporter
{
    public static void ExportAddressSpecificToCsv(Stream darJsonStream, Stream outputStream)
    {
        Console.Write("\rSearching...");
        using var sw = new StreamWriter(outputStream, Encoding.UTF8, 65536, leaveOpen: true);
        sw.WriteLine("unitAddressId;;buildingAddressId;;;;floor;door");

        using var sr = new StreamReader(darJsonStream);
        using var reader = new JsonTextReader(sr);
        int counter = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "AdresseList")
            {
                Console.Write("\rReading AdresseList");
                reader.Read(); // Move to StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;

                while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                {
                    if (reader.TokenType == JsonToken.StartObject)
                    {
                        string unitAddressId = null, buildingAddressId = null, floor = null, door = null;
                        
                        while (reader.Read() && reader.TokenType != JsonToken.EndObject)
                        {
                            if (reader.TokenType == JsonToken.PropertyName)
                            {
                                var prop = (string)reader.Value;
                                reader.Read();
                                
                                if (prop == "id_lokalId" && reader.TokenType == JsonToken.String)
                                    unitAddressId = (string)reader.Value;
                                else if (prop == "husnummer" && reader.TokenType == JsonToken.String)
                                    buildingAddressId = (string)reader.Value;
                                else if (prop == "etagebetegnelse" && reader.TokenType == JsonToken.String)
                                    floor = (string)reader.Value;
                                else if (prop == "dørbetegnelse" && reader.TokenType == JsonToken.String)
                                    door = (string)reader.Value;
                                else
                                    reader.Skip();
                            }
                        }
                        
                        sw.WriteLine($"{unitAddressId ?? ""};;{buildingAddressId ?? ""};;;;{floor ?? ""};{door ?? ""}");
                        counter++;
                        
                        if (counter % 10000 == 0)
                            Console.Write($"\rWriting AddressSpecific.csv: {counter}");
                    }
                }
                Console.WriteLine($"\rDone writing AddressSpecific.csv: {counter}");
                break;
            }
        }
        
        sw.Flush();
    }
}
