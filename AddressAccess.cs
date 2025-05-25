using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DatafordelerConverter;

public class AddressAccess
{
    public string? AddressAccessIdentifier { get; set; } // Husnummer - Adgangspunkt
    public string? StreetBuildingIdentifier { get; set; } // Husnummer - Husnummertekst
    public string? BuildingName { get; set; }
    public string? MunicipalityCode { get; set; } // NavngivenVejKommunedelList - kommune
    public string? StreetCode { get; set; } // NavngivenVejKommunedelList - vejkode
    public string? PostCodeIdentifier { get; set; } // Postnummer - Postnummer
    public string? DistrictName { get; set; } // Postnummer - Navn

    /// <summary>
    /// Exports the AddressAccess data to a CSV file from the JSON file.
    /// </summary>
    /// <param name="jsonFile"></param>
    /// <param name="csvFile"></param>
    /// <param name="vejKommunedelLookup"></param>
    public static void ExportHusnummerToCsv(string jsonFile, string csvFile, Dictionary<string, (string kommune, string vejkode)> vejKommunedelLookup, Dictionary<string, (string postnr, string navn)> postnummerLookup)
    {
        var addressAccessList = BuildAddressAccessList(jsonFile, postnummerLookup, vejKommunedelLookup);
        // Find ejerlav and jordstykke
        // Find adressepunkt
        WriteAddressAccessListToCsv(addressAccessList, csvFile);
    }

    /// <summary>
    /// Builds a lookup dictionary for PostnummerList from the JSON file.
    /// </summary>
    /// <param name="jsonFile"></param>
    /// <returns></returns>
    private static Dictionary<string, (string postnr, string navn)> BuildPostnummerLookup(string jsonFile)
    {
        Console.Write($"\rSearching...");
        var lookup = new Dictionary<string, (string postnr, string navn)>();
        using var sr = new StreamReader(jsonFile);
        using var reader = new JsonTextReader(sr);
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "PostnummerList")
            {
                Console.Write("\rLoading PostnummerList");
                reader.Read(); // Move to StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;
                var array = JArray.Load(reader);
                foreach (var item in array)
                {
                    var id = (string?)item["id_lokalId"];
                    var postnr = (string?)item["postnr"];
                    var navn = (string?)item["navn"];
                    if (!string.IsNullOrEmpty(id))
                        lookup[id] = (postnr ?? "", navn ?? "");
                }
                break;
            }
        }
        Console.WriteLine($"\rDone loading PostnummerList - {lookup.Count} items.");
        return lookup;
    }

    /// <summary>
    /// Builds a list of AddressAccess objects from the JSON file.
    /// </summary>
    /// <param name="jsonFile"></param>
    /// <param name="postnummerLookup"></param>
    /// <param name="vejKommunedelLookup"></param>
    /// <returns></returns>
    private static List<AddressAccess> BuildAddressAccessList(
        string jsonFile,
        Dictionary<string, (string postnr, string navn)> postnummerLookup,
        Dictionary<string, (string kommune, string vejkode)> vejKommunedelLookup)
    {
        Console.Write("\rSearching...");
        var list = new List<AddressAccess>(capacity: 4_000_000);
        using var sr = new StreamReader(jsonFile);
        using var reader = new JsonTextReader(sr);
        var counter = 0;
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "HusnummerList")
            {
                Console.Write("\rReading HusnummerList");
                reader.Read(); // StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;

                while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                {
                    if (reader.TokenType == JsonToken.StartObject)
                    {
                        string adgangspunkt = null, husnummertekst = null, postnummer = null, navngivenVej = null;
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
                                    case "navngivenVej":
                                        navngivenVej = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
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
                        string kommune = null, vejkode = null;
                        if (!string.IsNullOrEmpty(navngivenVej) && vejKommunedelLookup.TryGetValue(navngivenVej, out var vk))
                        {
                            kommune = vk.kommune;
                            vejkode = vk.vejkode;
                        }
                        list.Add(new AddressAccess
                        {
                            AddressAccessIdentifier = adgangspunkt,
                            StreetBuildingIdentifier = husnummertekst,
                            BuildingName = null,
                            MunicipalityCode = kommune,
                            StreetCode = vejkode,
                            PostCodeIdentifier = postnr,
                            DistrictName = navn
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
    /// Writes the AddressAccess list to a CSV file.
    /// </summary>
    /// <param name="list"></param>
    /// <param name="csvFile"></param>
    private static void WriteAddressAccessListToCsv(List<AddressAccess> list, string csvFile)
    {
        using var sw = new StreamWriter(csvFile, false, System.Text.Encoding.UTF8);
        sw.WriteLine("AddressAccessIdentifier;StreetBuildingIdentifier;BuildingName;MunicipalityCode;StreetCode;PostCodeIdentifier;DistrictName");
        var counter = 0;
        foreach (var item in list)
        {
            sw.WriteLine($"{item.AddressAccessIdentifier};" +
                         $"{item.StreetBuildingIdentifier};" +
                         $"{item.BuildingName};" +
                         $"{item.MunicipalityCode};" +
                         $"{item.StreetCode};" +
                         $"{item.PostCodeIdentifier};" +
                         $"{item.DistrictName}");
            counter++;
            if (counter % 1000 == 0)
                Console.Write($"\rWriting to AddressAccess.csv: {counter}");
        }
        Console.WriteLine($"\rDone writing to AddressAccess.csv: {counter}");
    }
}
