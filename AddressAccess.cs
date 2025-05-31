using System.Text;
using Newtonsoft.Json;

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

    public static async Task ExportHusnummerToCsvAsync(
        Stream darJsonStream,
        Stream matJsonStream,
        Stream outputStream,
        Dictionary<string, (string kommune, string vejkode)> vejKommunedelLookup,
        Dictionary<string, (string postnr, string navn)> postnummerLookup)
    {
        var overallStartTime = DateTime.Now;
        Console.WriteLine($"[{overallStartTime:HH:mm:ss}] *** Building lookup dictionaries ***");
        
        // Build matrikel lookup first
        var matrikelStartTime = DateTime.Now;
        Console.WriteLine($"[{matrikelStartTime:HH:mm:ss}] Starting matrikel lookup build...");
        var matrikelLookup = MatDataLoader.BuildMatrikelDict(matJsonStream);
        var matrikelEndTime = DateTime.Now;
        var matrikelDuration = matrikelEndTime - matrikelStartTime;
        Console.WriteLine($"[{matrikelEndTime:HH:mm:ss}] Matrikel lookup completed. Duration: {matrikelDuration.TotalSeconds:F2}s");
        
        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] *** Processing DAR data and writing CSV ***");
        
        // Process both AdressepunktList and HusnummerList in a single pass
        var darStartTime = DateTime.Now;
        Console.WriteLine($"[{darStartTime:HH:mm:ss}] Starting DAR data processing...");
        var (adressepunktPositionLookup, addressAccessList) = ProcessDarDataSinglePass(darJsonStream, vejKommunedelLookup, postnummerLookup);
        var darEndTime = DateTime.Now;
        var darDuration = darEndTime - darStartTime;
        Console.WriteLine($"[{darEndTime:HH:mm:ss}] DAR data processing completed. Duration: {darDuration.TotalSeconds:F2}s");
        
        using var sw = new StreamWriter(outputStream, Encoding.UTF8, 65536, leaveOpen: true);
        sw.WriteLine("AddressAccessIdentifier;StreetBuildingIdentifier;BuildingName;MunicipalityCode;StreetCode;PostCodeIdentifier;CadastralDistrictName;LandParcelIdentifier;?;DistrictName;ETRS89utm32Easting;ETRS89utm32Northing;AddressTextAngleMeasure;WGS84GeographicLatitude;WGS84GeographicLongitude;GeometryDDKNcell100mText;GeometryDDKNcell1kmText;GeometryDDKNcell10kmText");

        var csvStartTime = DateTime.Now;
        Console.WriteLine($"[{csvStartTime:HH:mm:ss}] Starting CSV enrichment and writing...");
        
        var counter = 0;
        
        foreach (var addressAccess in addressAccessList)
        {
            // Enrich with matrikel data
            if (matrikelLookup.TryGetValue(addressAccess.Jordstykke ?? "", out var matrikelData))
            {
                addressAccess.LandParcelIdentifier = matrikelData.matrikelnummer;
                addressAccess.CadastralDistrictName = matrikelData.ejerlavsnavn;
            }

            // Enrich with position data
            if (adressepunktPositionLookup.TryGetValue(addressAccess.AddressAccessIdentifier ?? "", out var position) &&
                position.Length > 7 && position.StartsWith("POINT(", StringComparison.OrdinalIgnoreCase))
            {
                var coords = position[6..^1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (coords.Length == 2)
                {
                    addressAccess.ETRS89utm32Easting = coords[0];
                    addressAccess.ETRS89utm32Northing = coords[1];
                }
            }

            // Write to CSV
            sw.WriteLine($"{addressAccess.AddressAccessIdentifier ?? ""};{addressAccess.StreetBuildingIdentifier ?? ""};{addressAccess.BuildingName ?? ""};{addressAccess.MunicipalityCode ?? ""};{addressAccess.StreetCode ?? ""};{addressAccess.PostCodeIdentifier ?? ""};{addressAccess.CadastralDistrictName ?? ""};{addressAccess.LandParcelIdentifier ?? ""}; ;{addressAccess.DistrictName ?? ""};{addressAccess.ETRS89utm32Easting ?? ""};{addressAccess.ETRS89utm32Northing ?? ""};;;;;");
            
            counter++;
            if (counter % 10000 == 0)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Writing CSV data: {counter}");
                // Flush periodically to avoid memory buildup
                await sw.FlushAsync();
            }
        }
        
        await sw.FlushAsync();
        var csvEndTime = DateTime.Now;
        var csvDuration = csvEndTime - csvStartTime;
        var overallDuration = csvEndTime - overallStartTime;
        Console.WriteLine($"[{csvEndTime:HH:mm:ss}] Done writing AddressAccess.csv: {counter} records. CSV Duration: {csvDuration.TotalSeconds:F2}s");
        Console.WriteLine($"[{csvEndTime:HH:mm:ss}] Total AddressAccess processing time: {overallDuration.TotalSeconds:F2}s");
    }

    private static AddressAccess? ProcessHusnummerObject(
        JsonTextReader reader,
        Dictionary<string, (string kommune, string vejkode)> vejKommunedelLookup,
        Dictionary<string, (string postnr, string navn)> postnummerLookup)
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
                    case "adgangspunkt" when reader.TokenType == JsonToken.String:
                        adgangspunkt = (string)reader.Value;
                        break;
                    case "husnummertekst" when reader.TokenType == JsonToken.String:
                        husnummertekst = (string)reader.Value;
                        break;
                    case "postnummer" when reader.TokenType == JsonToken.String:
                        postnummer = (string)reader.Value;
                        break;
                    case "navngivenVej" when reader.TokenType == JsonToken.String:
                        navngivenVej = (string)reader.Value;
                        break;
                    case "jordstykke" when reader.TokenType == JsonToken.String:
                        jordstykke = (string)reader.Value;
                        break;
                    default:
                        reader.Skip();
                        break;
                }
            }
        }

        var (postnr, navn) = postnummerLookup.TryGetValue(postnummer ?? "", out var pn) ? pn : ("", "");
        var (kommune, vejkode) = vejKommunedelLookup.TryGetValue(navngivenVej ?? "", out var vk) ? vk : (null, null);

        return new AddressAccess
        {
            AddressAccessIdentifier = adgangspunkt,
            StreetBuildingIdentifier = husnummertekst,
            MunicipalityCode = kommune,
            StreetCode = vejkode,
            PostCodeIdentifier = postnr,
            Jordstykke = jordstykke,
            DistrictName = navn
        };
    }

    private static (Dictionary<string, string> adressepunktPositionLookup, List<AddressAccess> addressAccessList) ProcessDarDataSinglePass(
        Stream darJsonStream,
        Dictionary<string, (string kommune, string vejkode)> vejKommunedelLookup,
        Dictionary<string, (string postnr, string navn)> postnummerLookup)
    {
        var adressepunktPositionLookup = new Dictionary<string, string>(500_000);
        var addressAccessList = new List<AddressAccess>();
        
        using var sr = new StreamReader(darJsonStream);
        using var reader = new JsonTextReader(sr);
        
        bool adressepunktProcessed = false;
        bool husnummerProcessed = false;
        
        while (reader.Read() && (!adressepunktProcessed || !husnummerProcessed))
        {
            if (reader.TokenType == JsonToken.PropertyName)
            {
                var propertyName = (string)reader.Value;
                
                if (propertyName == "AdressepunktList" && !adressepunktProcessed)
                {
                    var startTime = DateTime.Now;
                    Console.WriteLine($"[{startTime:HH:mm:ss}] Searching...AdressepunktList");
                    reader.Read(); // Move to StartArray
                    if (reader.TokenType == JsonToken.StartArray)
                    {
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Loading...AdressepunktList");
                        int counter = 0;
                        while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                        {
                            if (reader.TokenType == JsonToken.StartObject)
                            {
                                string id = null, position = null;
                                
                                while (reader.Read() && reader.TokenType != JsonToken.EndObject)
                                {
                                    if (reader.TokenType == JsonToken.PropertyName)
                                    {
                                        var prop = (string)reader.Value;
                                        reader.Read();
                                        
                                        if (prop == "id_lokalId" && reader.TokenType == JsonToken.String)
                                            id = (string)reader.Value;
                                        else if (prop == "position" && reader.TokenType == JsonToken.String)
                                            position = (string)reader.Value;
                                        else
                                            reader.Skip();
                                    }
                                }
                                
                                if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(position))
                                {
                                    adressepunktPositionLookup[id] = position;
                                    counter++;
                                    if (counter % 10000 == 0)
                                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Loading...AdressepunktList: {counter}");
                                }
                            }
                        }
                        var endTime = DateTime.Now;
                        var duration = endTime - startTime;
                        Console.WriteLine($"[{endTime:HH:mm:ss}] Done loading AdressepunktList - {adressepunktPositionLookup.Count} items. Duration: {duration.TotalSeconds:F2}s");
                        adressepunktProcessed = true;
                    }
                }
                else if (propertyName == "HusnummerList" && !husnummerProcessed)
                {
                    var startTime = DateTime.Now;
                    Console.WriteLine($"[{startTime:HH:mm:ss}] Searching...HusnummerList");
                    reader.Read(); // Move to StartArray
                    if (reader.TokenType == JsonToken.StartArray)
                    {
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Loading...HusnummerList");
                        int counter = 0;
                        while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                        {
                            if (reader.TokenType == JsonToken.StartObject)
                            {
                                var addressAccess = ProcessHusnummerObject(reader, vejKommunedelLookup, postnummerLookup);
                                if (addressAccess != null)
                                {
                                    addressAccessList.Add(addressAccess);
                                    counter++;
                                    if (counter % 10000 == 0)
                                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Loading...HusnummerList: {counter}");
                                }
                            }
                        }
                        var endTime = DateTime.Now;
                        var duration = endTime - startTime;
                        Console.WriteLine($"[{endTime:HH:mm:ss}] Done loading HusnummerList - {addressAccessList.Count} items. Duration: {duration.TotalSeconds:F2}s");
                        husnummerProcessed = true;
                    }
                }
            }
        }
        
        return (adressepunktPositionLookup, addressAccessList);
    }
}
