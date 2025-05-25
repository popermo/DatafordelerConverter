using Newtonsoft.Json;

namespace DatafordelerConverter;

public static class PostCode
{
    /// <summary>
    /// Orchestrates the export of PostnummerList to CSV.
    /// </summary>
    public static void ExportPostnummerToCsv(string jsonFile, string csvFile)
    {
        ExportPostnummerListCsv(jsonFile, csvFile);
    }

    /// <summary>
    /// Streams PostnummerList and writes the CSV.
    /// </summary>
    private static void ExportPostnummerListCsv(string jsonFile, string csvFile)
    {
        using var sr = new StreamReader(jsonFile);
        using var reader = new JsonTextReader(sr);
        using var sw = new StreamWriter(csvFile, false, System.Text.Encoding.UTF8);
        var counter = 0;
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
                            counter++;
                            sw.WriteLine($"{postnr};;{navn}");
                        }
                    }
                    Console.Write($"\rWriting to PostCode.csv: {counter}");
                }
                break; // Done with PostnummerList
            }
        }
        sw.Flush();
        Console.WriteLine($"\rDone writing PostCode.csv - {counter}");
    }
}