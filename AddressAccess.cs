using Newtonsoft.Json;

namespace DatafordelerConverter;

public class AddressAccess
{
    public string? AddressAccessIdentifier { get; set; } // HusnummerList - adgangspunkt
    public string? StreetBuildingIdentifier { get; set; } // Husnummer - husnummertekst
    public string? BuildingName { get; set; }
    public string? MunicipalityCode { get; set; } // NavngivenVejKommunedelList - kommune
    public string? StreetCode { get; set; } // NavngivenVejKommunedelList - vejkode
    public string? PostCodeIdentifier { get; set; } // Postnummer - postnummer
    public string? DistrictName { get; set; } // Postnummer - navn
    public string? Jordstykke { get; set; } // HusnummerList - jordstykke
    public string? LandParcelIdentifier { get; set; } // JordstykkeList - matrikelnummer
    public string? CadastralDistrictName { get; set; } // EjerlavList - ejerlavsnavn
    public string? ETRS89utm32Easting { get; set; } // AdressepunktList - position
    public string? ETRS89utm32Northing { get; set; } // AdressepunktList - position


    /// <summary>
    /// Exports the AddressAccess data to a CSV file from the JSON file.
    /// </summary>
    /// <param name="jsonDarFile"></param>
    /// <param name="jsonMatFile"></param>
    /// <param name="csvFile"></param>
    /// <param name="vejKommunedelLookup"></param>
    /// <param name="postnummerLookup"></param>
    public static void ExportHusnummerToCsv(
        string jsonDarFile,
        string jsonMatFile,
        string csvFile,
        Dictionary<string, (string kommune, string vejkode)> vejKommunedelLookup,
        Dictionary<string, (string postnr, string navn)> postnummerLookup)
    {
        var addressAccessList = BuildAddressAccessList(jsonDarFile, postnummerLookup, vejKommunedelLookup);
        var matrikelLookup = MatDataLoader.BuildMatrikelDict(jsonMatFile);
        var adressepunktPositionLookup = BuildAdressepunktPositionLookup(jsonDarFile);

        EnrichAddressAccessList(addressAccessList, matrikelLookup, adressepunktPositionLookup);

        WriteAddressAccessListToCsv(addressAccessList, csvFile);
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
                        string adgangspunkt = null, husnummertekst = null, postnummer = null, navngivenVej = null, jordstykke = null;
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
                                    case "jordstykke":
                                        jordstykke = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
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
                            Jordstykke = jordstykke,
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
    /// Builds a lookup dictionary for AdressepunktList from the JSON file.
    /// </summary>
    /// <param name="darJsonFile"></param>
    /// <returns></returns>
    private static Dictionary<string, string> BuildAdressepunktPositionLookup(string darJsonFile)
    {
        var lookup = new Dictionary<string, string>();
        using var sr = new StreamReader(darJsonFile);
        using var reader = new JsonTextReader(sr);
        Console.Write("\rSearching...AdressepunktList");
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "AdressepunktList")
            {
                reader.Read(); // Move to StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;
                Console.Write("\rLoading...AdressepunktList");
                int counter = 0;
                while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                {
                    if (reader.TokenType == JsonToken.StartObject)
                    {
                        string? id = null;
                        string? position = null;
                        while (reader.Read() && reader.TokenType != JsonToken.EndObject)
                        {
                            if (reader.TokenType == JsonToken.PropertyName)
                            {
                                var prop = (string)reader.Value;
                                reader.Read();
                                switch (prop)
                                {
                                    case "id_lokalId":
                                        id = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    case "position":
                                        position = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    default:
                                        reader.Skip();
                                        break;
                                }
                            }
                        }
                        if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(position))
                        {
                            lookup[id] = position;
                            counter++;
                            if (counter % 1000 == 0)
                                Console.Write($"\rLoading...AdressepunktList: {counter}");
                        }
                    }
                }
                Console.WriteLine($"\rDone loading AdressepunktList - {lookup.Count} items.");
                break;
            }
        }
        return lookup;
    }

    /// <summary>
    /// Enriches the AddressAccess list with additional data from matrikel and adressepunkt position lookups.
    /// </summary>
    /// <param name="addressAccessList"></param>
    /// <param name="matrikelLookup"></param>
    /// <param name="adressepunktPositionLookup"></param>
    private static void EnrichAddressAccessList(
        List<AddressAccess> addressAccessList,
        Dictionary<string, (string matrikelnummer, string ejerlavsnavn)> matrikelLookup,
        Dictionary<string, string> adressepunktPositionLookup)
    {
        var counter = 0;
        foreach (var item in addressAccessList)
        {
            // Matrikel data
            if (item.Jordstykke != null && matrikelLookup.TryGetValue(item.Jordstykke, out var matrikelData))
            {
                item.LandParcelIdentifier = matrikelData.matrikelnummer;
                item.CadastralDistrictName = matrikelData.ejerlavsnavn;
            }
            else
            {
                item.LandParcelIdentifier = null;
                item.CadastralDistrictName = null;
            }

            // Adressepunkt position (ETRS89utm32Easting/Northing)
            if (!string.IsNullOrEmpty(item.AddressAccessIdentifier) &&
                adressepunktPositionLookup.TryGetValue(item.AddressAccessIdentifier, out var position) &&
                !string.IsNullOrEmpty(position))
            {
                // Expected format: "POINT(698217.056989288 6200618.321236086)"
                var start = position.IndexOf('(');
                var end = position.IndexOf(')');
                if (start >= 0 && end > start)
                {
                    var coords = position.Substring(start + 1, end - start - 1).Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (coords.Length == 2)
                    {
                        item.ETRS89utm32Easting = coords[0];
                        item.ETRS89utm32Northing = coords[1];
                    }
                }
            }
            else
            {
                item.ETRS89utm32Easting = null;
                item.ETRS89utm32Northing = null;
            }

            counter++;
            if (counter % 1000 == 0)
                Console.Write($"\rUpdating AddressAccess objects: {counter}");
        }
        Console.WriteLine($"\rDone updating AddressAccess objects: {counter}");
    }

    /// <summary>
    /// Writes the AddressAccess list to a CSV file.
    /// </summary>
    /// <param name="list"></param>
    /// <param name="csvFile"></param>
    private static void WriteAddressAccessListToCsv(List<AddressAccess> list, string csvFile)
    {
        using var sw = new StreamWriter(csvFile, false, System.Text.Encoding.UTF8);
        sw.WriteLine("AddressAccessIdentifier;" +
                     "StreetBuildingIdentifier;" +
                     "BuildingName;MunicipalityCode;" +
                     "StreetCode;" +
                     "PostCodeIdentifier;" +
                     "CadastralDistrictName;" +
                     "LandParcelIdentifier;" +
                     "?;" +
                     "DistrictName;" +
                     "ETRS89utm32Easting;" +
                     "ETRS89utm32Northing;" +
                     "AddressTextAngleMeasure;" +
                     "WGS84GeographicLatitude;" +
                     "WGS84GeographicLongitude;" +
                     "GeometryDDKNcell100mText;" +
                     "GeometryDDKNcell1kmText;" +
                     "GeometryDDKNcell10kmText");
        var counter = 0;
        foreach (var item in list)
        {
            sw.WriteLine($"{item.AddressAccessIdentifier};" +
                         $"{item.StreetBuildingIdentifier};" +
                         $"{item.BuildingName};" +
                         $"{item.MunicipalityCode};" +
                         $"{item.StreetCode};" +
                         $"{item.PostCodeIdentifier};" +
                         $"{item.CadastralDistrictName};" +
                         $"{item.LandParcelIdentifier};" +
                         ";" + 
                         $"{item.DistrictName};" +
                         $"{item.ETRS89utm32Easting};" + 
                         $"{item.ETRS89utm32Northing};" + 
                         ";;;;;");
            counter++;
            if (counter % 1000 == 0)
                Console.Write($"\rWriting to AddressAccess.csv: {counter}");
        }
        Console.WriteLine($"\rDone writing to AddressAccess.csv: {counter}");
    }
}
