using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Diagnostics.Metrics;

namespace DatafordelerConverter;

public class AddressAccess
{
    public string? Adgangspunkt { get; set; }
    public string? Husnummertekst { get; set; }
    public string? Postnr { get; set; }
    public string? Navn { get; set; }

    /// <summary>
    /// Orchestrates the export of Husnummer (AddressAccess) data to CSV.
    /// </summary>
    public static void ExportHusnummerToCsv(string jsonFile, string csvFile)
    {
        var postnummerLookup = BuildPostnummerLookup(jsonFile);
        var addressAccessList = BuildAddressAccessListFromJson(jsonFile, postnummerLookup);
        WriteAddressAccessListToCsv(addressAccessList, csvFile);
    }

    /// <summary>
    /// Loads PostnummerList into a lookup dictionary.
    /// </summary>
    private static Dictionary<string, (string postnr, string navn)> BuildPostnummerLookup(string jsonFile)
    {
        var counter = 0;
        var lookup = new Dictionary<string, (string postnr, string navn)>();
        using var sr = new StreamReader(jsonFile);
        using var reader = new JsonTextReader(sr);
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "PostnummerList")
            {
                reader.Read(); // StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;

                var array = JArray.Load(reader);
                foreach (var item in array)
                {
                    var id = (string?)item["id_lokalId"];
                    var postnr = (string?)item["postnr"];
                    var navn = (string?)item["navn"];
                    if (!string.IsNullOrEmpty(id))
                    {
                        counter++;
                        lookup[id] = (postnr ?? "", navn ?? "");
                        Console.Write($"\rLoading PostnummerDictionary: {counter}");
                    }
                }
                break;
            }
        }
        Console.WriteLine($"\rDone loading PostnummerDictionary - {counter} items.");
        return lookup;
    }

    /// <summary>
    /// Streams HusnummerList and builds the AddressAccess list.
    /// </summary>
    private static List<AddressAccess> BuildAddressAccessListFromJson(
        string jsonFile,
        Dictionary<string, (string postnr, string navn)> postnummerLookup)
    {
        var list = new List<AddressAccess>(capacity: 4_000_000); // Pre-allocate if you know the size
        using var sr = new StreamReader(jsonFile);
        using var reader = new JsonTextReader(sr);
        int counter = 0;
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "HusnummerList")
            {
                reader.Read(); // StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;

                while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                {
                    if (reader.TokenType == JsonToken.StartObject)
                    {
                        string adgangspunkt = null, husnummertekst = null, postnummer = null;
                        while (reader.Read() && reader.TokenType != JsonToken.EndObject)
                        {
                            if (reader.TokenType == JsonToken.PropertyName)
                            {
                                var prop = (string)reader.Value;
                                reader.Read();
                                switch (prop)
                                {
                                    case "adgangspunkt":
                                        adgangspunkt = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    case "husnummertekst":
                                        husnummertekst = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    case "postnummer":
                                        postnummer = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    default:
                                        reader.Skip();
                                        break;
                                }
                            }
                        }
                        string postnr = "", navn = "";
                        if (!string.IsNullOrEmpty(postnummer) && postnummerLookup.TryGetValue(postnummer, out var pn))
                        {
                            postnr = pn.postnr;
                            navn = pn.navn;
                        }
                        list.Add(new AddressAccess
                        {
                            Adgangspunkt = adgangspunkt,
                            Husnummertekst = husnummertekst,
                            Postnr = postnr,
                            Navn = navn
                        });
                        counter++;
                        if (counter % 1000 == 0)
                            Console.Write($"\rCreating AddressAccess objects: {counter}");
                    }
                }
                Console.WriteLine($"\rDone creating AddressAccess objects: {counter}");
                break;
            }
        }
        return list;
    }

    /// <summary>
    /// Writes a list of AddressAccess objects to a CSV file.
    /// </summary>
    private static void WriteAddressAccessListToCsv(List<AddressAccess> list, string csvFile)
    {
        using var sw = new StreamWriter(csvFile, false, System.Text.Encoding.UTF8);
        sw.WriteLine("Adgangspunkt;Husnummertekst;Postnr;Navn");
        int counter = 0;
        foreach (var item in list)
        {
            sw.WriteLine($"{item.Adgangspunkt};{item.Husnummertekst};{item.Postnr};{item.Navn}");
            counter++;
            if (counter % 1000 == 0)
                Console.Write($"\rWriting AddressAccess objects to AddressAccess.csv: {counter}");
        }
        Console.WriteLine($"\rDone writing AddressAccess objects to AddressAccess.csv: {counter}");
    }
}