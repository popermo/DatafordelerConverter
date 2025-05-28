using System.Text;
using System.Text.Json;

namespace DatafordelerConverter;

public static class AddressSpecificExporter
{
    public static void ExportAddressSpecificToCsv(Stream darJsonStream, string csvFile)
    {
        Console.Write("\rSearching...");
        using var sw = new StreamWriter(csvFile, false, Encoding.UTF8, 65536);
        sw.WriteLine("unitAddressId;;buildingAddressId;;;;floor;door");

        var buffer = new byte[65536];
        int bytesRead;
        var ms = new MemoryStream();

        while ((bytesRead = darJsonStream.Read(buffer, 0, buffer.Length)) > 0)
        {
            ms.Write(buffer, 0, bytesRead);
        }
        ms.Position = 0;

        var options = new JsonReaderOptions { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip };
        var reader = new Utf8JsonReader(ms.ToArray(), options);
        int counter = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.PropertyName && reader.GetString() == "AdresseList")
            {
                Console.Write("\rReading AdresseList");
                reader.Read(); // StartArray
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        string unitAddressId = null, buildingAddressId = null, floor = null, door = null;
                        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                        {
                            if (reader.TokenType == JsonTokenType.PropertyName)
                            {
                                var prop = reader.GetString();
                                reader.Read();
                                if (prop == "id_lokalId" && reader.TokenType == JsonTokenType.String)
                                    unitAddressId = reader.GetString();
                                else if (prop == "husnummer" && reader.TokenType == JsonTokenType.String)
                                    buildingAddressId = reader.GetString();
                                else if (prop == "etagebetegnelse" && reader.TokenType == JsonTokenType.String)
                                    floor = reader.GetString();
                                else if (prop == "dørbetegnelse" && reader.TokenType == JsonTokenType.String)
                                    door = reader.GetString();
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
    }
}