using Newtonsoft.Json;

namespace DatafordelerConverter;

public static class AddressSpecificExporter
{
    /// <summary>
    /// Exports the AddressSpecific data from a DAR JSON file to a CSV file.
    /// </summary>
    /// <param name="darJsonFile"></param>
    /// <param name="csvFile"></param>
    public static void ExportAddressSpecificToCsv(string darJsonFile, string csvFile)
    {
        using var sr = new StreamReader(darJsonFile);
        using var reader = new JsonTextReader(sr);
        using var sw = new StreamWriter(csvFile, false, System.Text.Encoding.UTF8);

        sw.WriteLine("unitAddressId;;buildingAddressId;;;;floor;door");
        int counter = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "AdresseList")
            {
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
                                switch (prop)
                                {
                                    case "id_lokalId":
                                        unitAddressId = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    case "husnummer":
                                        buildingAddressId = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    case "etagebetegnelse":
                                        floor = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    case "dørbetegnelse":
                                        door = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    default:
                                        reader.Skip();
                                        break;
                                }
                            }
                        }
                        sw.WriteLine($"{unitAddressId};;{buildingAddressId};;;;{floor};{door}");
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
