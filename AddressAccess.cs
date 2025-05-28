using System.Text;
using System.Text.Json;

namespace DatafordelerConverter;

public class AddressAccess
{
    public string? AddressAccessIdentifier { get; set; }
    public string? StreetBuildingIdentifier { get; set; }
    public string? BuildingName { get; set; }
    public string? MunicipalityCode { get; set; }
    public string? StreetCode { get; set; }
    public string? PostCodeIdentifier { get; set; }
    public string? DistrictName { get; set; }
    public string? Jordstykke { get; set; }
    public string? LandParcelIdentifier { get; set; }
    public string? CadastralDistrictName { get; set; }
    public string? ETRS89utm32Easting { get; set; }
    public string? ETRS89utm32Northing { get; set; }

    public static void ExportHusnummerToCsv(
        Stream darJsonStream,
        Stream matJsonStream,
        string csvFile,
        Dictionary<string, (string kommune, string vejkode)> vejKommunedelLookup,
        Dictionary<string, (string postnr, string navn)> postnummerLookup)
    {
        var addressAccessList = BuildAddressAccessList(darJsonStream, postnummerLookup, vejKommunedelLookup);
        darJsonStream.Position = 0; // Reset stream for second pass
        var matrikelLookup = MatDataLoader.BuildMatrikelDict(matJsonStream);
        var adressepunktPositionLookup = BuildAdressepunktPositionLookup(darJsonStream);

        EnrichAddressAccessList(addressAccessList, matrikelLookup, adressepunktPositionLookup);
        WriteAddressAccessListToCsv(addressAccessList, csvFile);
    }

    private static List<AddressAccess> BuildAddressAccessList(
        Stream jsonStream,
        Dictionary<string, (string postnr, string navn)> postnummerLookup,
        Dictionary<string, (string kommune, string vejkode)> vejKommunedelLookup)
    {
        Console.Write("\rSearching...");
        var list = new List<AddressAccess>(4_000_000);
        var buffer = new byte[65536];
        int bytesRead;
        var ms = new MemoryStream();

        while ((bytesRead = jsonStream.Read(buffer, 0, buffer.Length)) > 0)
        {
            ms.Write(buffer, 0, bytesRead);
        }
        ms.Position = 0;

        var options = new JsonReaderOptions { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip };
        var reader = new Utf8JsonReader(ms.ToArray(), options);
        var counter = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.PropertyName && reader.GetString() == "HusnummerList")
            {
                Console.Write("\rReading HusnummerList");
                reader.Read(); // StartArray
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        string adgangspunkt = null, husnummertekst = null, postnummer = null, navngivenVej = null, jordstykke = null;
                        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                        {
                            if (reader.TokenType == JsonTokenType.PropertyName)
                            {
                                var prop = reader.GetString();
                                reader.Read();
                                if (prop == "adgangspunkt" && reader.TokenType == JsonTokenType.String)
                                    adgangspunkt = reader.GetString();
                                else if (prop == "husnummertekst" && reader.TokenType == JsonTokenType.String)
                                    husnummertekst = reader.GetString();
                                else if (prop == "postnummer" && reader.TokenType == JsonTokenType.String)
                                    postnummer = reader.GetString();
                                else if (prop == "navngivenVej" && reader.TokenType == JsonTokenType.String)
                                    navngivenVej = reader.GetString();
                                else if (prop == "jordstykke" && reader.TokenType == JsonTokenType.String)
                                    jordstykke = reader.GetString();
                                else
                                    reader.Skip();
                            }
                        }

                        var (postnr, navn) = postnummerLookup.TryGetValue(postnummer ?? "", out var pn) ? pn : ("", "");
                        var (kommune, vejkode) = vejKommunedelLookup.TryGetValue(navngivenVej ?? "", out var vk) ? vk : (null, null);

                        list.Add(new AddressAccess
                        {
                            AddressAccessIdentifier = adgangspunkt,
                            StreetBuildingIdentifier = husnummertekst,
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

    private static Dictionary<string, string> BuildAdressepunktPositionLookup(Stream darJsonStream)
    {
        var lookup = new Dictionary<string, string>(500_000);
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
        Console.Write("\rSearching...AdressepunktList");
        int counter = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.PropertyName && reader.GetString() == "AdressepunktList")
            {
                Console.Write("\rLoading...AdressepunktList");
                reader.Read(); // StartArray
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        string id = null, position = null;
                        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                        {
                            if (reader.TokenType == JsonTokenType.PropertyName)
                            {
                                var prop = reader.GetString();
                                reader.Read();
                                if (prop == "id_lokalId" && reader.TokenType == JsonTokenType.String)
                                    id = reader.GetString();
                                else if (prop == "position" && reader.TokenType == JsonTokenType.String)
                                    position = reader.GetString();
                                else
                                    reader.Skip();
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

    private static void EnrichAddressAccessList(
        List<AddressAccess> addressAccessList,
        Dictionary<string, (string matrikelnummer, string ejerlavsnavn)> matrikelLookup,
        Dictionary<string, string> adressepunktPositionLookup)
    {
        int counter = 0;
        foreach (var item in addressAccessList)
        {
            if (matrikelLookup.TryGetValue(item.Jordstykke ?? "", out var matrikelData))
            {
                item.LandParcelIdentifier = matrikelData.matrikelnummer;
                item.CadastralDistrictName = matrikelData.ejerlavsnavn;
            }

            if (adressepunktPositionLookup.TryGetValue(item.AddressAccessIdentifier ?? "", out var position) &&
                position.Length > 7)
            {
                if (position.StartsWith("POINT(", StringComparison.OrdinalIgnoreCase))
                {
                    var coords = position[6..^1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    if (coords.Length == 2)
                    {
                        item.ETRS89utm32Easting = coords[0];
                        item.ETRS89utm32Northing = coords[1];
                    }
                }
            }

            counter++;
            if (counter % 1000 == 0)
                Console.Write($"\rUpdating AddressAccess objects: {counter}");
        }
        Console.WriteLine($"\rDone updating AddressAccess objects: {counter}");
    }

    private static void WriteAddressAccessListToCsv(List<AddressAccess> list, string csvFile)
    {
        using var sw = new StreamWriter(csvFile, false, Encoding.UTF8, 65536);
        sw.WriteLine("AddressAccessIdentifier;StreetBuildingIdentifier;BuildingName;MunicipalityCode;StreetCode;PostCodeIdentifier;CadastralDistrictName;LandParcelIdentifier;?;DistrictName;ETRS89utm32Easting;ETRS89utm32Northing;AddressTextAngleMeasure;WGS84GeographicLatitude;WGS84GeographicLongitude;GeometryDDKNcell100mText;GeometryDDKNcell1kmText;GeometryDDKNcell10kmText");

        int counter = 0;
        foreach (var item in list)
        {
            sw.WriteLine($"{item.AddressAccessIdentifier ?? ""};{item.StreetBuildingIdentifier ?? ""};{item.BuildingName ?? ""};{item.MunicipalityCode ?? ""};{item.StreetCode ?? ""};{item.PostCodeIdentifier ?? ""};{item.CadastralDistrictName ?? ""};{item.LandParcelIdentifier ?? ""}; ;{item.DistrictName ?? ""};{item.ETRS89utm32Easting ?? ""};{item.ETRS89utm32Northing ?? ""};;;;;");
            counter++;
            if (counter % 1000 == 0)
                Console.Write($"\rWriting to AddressAccess.csv: {counter}");
        }
        Console.WriteLine($"\rDone writing to AddressAccess.csv: {counter}");
    }
}