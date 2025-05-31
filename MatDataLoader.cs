using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DatafordelerConverter;

public static class MatDataLoader
{
    /// <summary>
    /// Builds a dictionary of matrikel data from the JSON stream.
    /// </summary>
    /// <param name="matJsonStream"></param>
    /// <returns></returns>
    public static Dictionary<string, (string matrikelnummer, string ejerlavsnavn)> BuildMatrikelDict(Stream matJsonStream)
    {
        // For Azure Storage streams that don't support seeking, we need to read both lookups in a single pass
        return BuildMatrikelDictSinglePass(matJsonStream);
    }

    /// <summary>
    /// Builds both EjerlavList and JordstykkeList lookups in a single pass through the stream.
    /// This is necessary for Azure Storage streams that don't support seeking.
    /// </summary>
    /// <param name="matJsonStream"></param>
    /// <returns></returns>
    private static Dictionary<string, (string matrikelnummer, string ejerlavsnavn)> BuildMatrikelDictSinglePass(Stream matJsonStream)
    {
        var ejerlavLookup = new Dictionary<string, string>();
        var jordstykkeResult = new Dictionary<string, (string, string)>();
        var today = DateTime.Now.Date;
        
        using var sr = new StreamReader(matJsonStream);
        using var reader = new JsonTextReader(sr);
        
        bool ejerlavProcessed = false;
        bool jordstykkeProcessed = false;
        
        while (reader.Read() && (!ejerlavProcessed || !jordstykkeProcessed))
        {
            if (reader.TokenType == JsonToken.PropertyName)
            {
                var propertyName = (string)reader.Value;
                
                if (propertyName == "EjerlavList" && !ejerlavProcessed)
                {
                    var startTime = DateTime.Now;
                    Console.WriteLine($"[{startTime:HH:mm:ss}] Searching...EjerlavList");
                    reader.Read(); // Move to StartArray
                    if (reader.TokenType == JsonToken.StartArray)
                    {
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Loading...EjerlavList");
                        var array = JArray.Load(reader);
                        foreach (var item in array)
                        {
                            var id = (string?)item["id_lokalId"];
                            var navn = (string?)item["ejerlavsnavn"];
                            if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(navn))
                                ejerlavLookup[id] = navn;
                        }
                        var endTime = DateTime.Now;
                        var duration = endTime - startTime;
                        Console.WriteLine($"[{endTime:HH:mm:ss}] Done loading EjerlavList - {ejerlavLookup.Count} items. Duration: {duration.TotalSeconds:F2}s");
                        ejerlavProcessed = true;
                    }
                }
                else if (propertyName == "JordstykkeList" && !jordstykkeProcessed)
                {
                    var startTime = DateTime.Now;
                    Console.WriteLine($"[{startTime:HH:mm:ss}] Searching...JordstykkeList");
                    reader.Read(); // Move to StartArray
                    if (reader.TokenType == JsonToken.StartArray)
                    {
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Loading...JordstykkeList");
                        int counter = 0;
                        while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                        {
                            if (reader.TokenType == JsonToken.StartObject)
                            {
                                string? id = null, matrikelnummer = null, ejerlavLokalId = null;
                                DateTime? virkningFra = null, virkningTil = null, registreringFra = null, registreringTil = null;
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
                                            case "matrikelnummer":
                                                matrikelnummer = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                                break;
                                            case "ejerlavLokalId":
                                                ejerlavLokalId = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                                break;
                                            case "virkningFra":
                                                virkningFra = reader.TokenType == JsonToken.Date
                                                    ? (DateTime?)reader.Value
                                                    : DateTime.TryParse(reader.Value?.ToString(), out var vf) ? vf : null;
                                                break;
                                            case "virkningTil":
                                                virkningTil = reader.TokenType == JsonToken.Date
                                                    ? (DateTime?)reader.Value
                                                    : DateTime.TryParse(reader.Value?.ToString(), out var vt) ? vt : null;
                                                break;
                                            case "registreringFra":
                                                registreringFra = reader.TokenType == JsonToken.Date
                                                    ? (DateTime?)reader.Value
                                                    : DateTime.TryParse(reader.Value?.ToString(), out var rf) ? rf : null;
                                                break;
                                            case "registreringTil":
                                                registreringTil = reader.TokenType == JsonToken.Date
                                                    ? (DateTime?)reader.Value
                                                    : DateTime.TryParse(reader.Value?.ToString(), out var rt) ? rt : null;
                                                break;
                                            default:
                                                reader.Skip();
                                                break;
                                        }
                                    }
                                }
                                // Parse and filter dates
                                if (virkningFra == null || registreringFra == null)
                                    continue;
                                if (virkningFra.Value.Date > today || registreringFra.Value.Date > today)
                                    continue;
                                if (virkningTil != null || registreringTil != null)
                                    continue;

                                if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(matrikelnummer) && !string.IsNullOrEmpty(ejerlavLokalId))
                                {
                                    if (ejerlavLookup.TryGetValue(ejerlavLokalId, out var ejerlavsnavn))
                                    {
                                        jordstykkeResult[id] = (matrikelnummer, ejerlavsnavn);
                                        counter++;
                                        if (counter % 10000 == 0)
                                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Loading...JordstykkeList: {counter}");
                                    }
                                }
                            }
                        }
                        var endTime = DateTime.Now;
                        var duration = endTime - startTime;
                        Console.WriteLine($"[{endTime:HH:mm:ss}] Done loading JordstykkeList - {jordstykkeResult.Count} items. Duration: {duration.TotalSeconds:F2}s");
                        jordstykkeProcessed = true;
                    }
                }
            }
        }
        
        return jordstykkeResult;
    }

    /// <summary>
    /// Builds a lookup dictionary for EjerlavList from the JSON stream.
    /// </summary>
    /// <param name="matJsonStream"></param>
    /// <returns></returns>
    private static Dictionary<string, string> BuildEjerlavLookup(Stream matJsonStream)
    {
        var lookup = new Dictionary<string, string>();
        using var sr = new StreamReader(matJsonStream);
        using var reader = new JsonTextReader(sr);
        Console.Write("\rSearching...EjerlavList");
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "EjerlavList")
            {
                reader.Read(); // Move to StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;
                Console.Write("\rLoading...EjerlavList");
                var array = JArray.Load(reader);
                foreach (var item in array)
                {
                    var id = (string?)item["id_lokalId"];
                    var navn = (string?)item["ejerlavsnavn"];
                    if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(navn))
                        lookup[id] = navn;
                }
                break;
            }
        }
        Console.WriteLine($"\rDone loading EjerlavList - {lookup.Count} items.");
        return lookup;
    }

    /// <summary>
    /// Builds a lookup dictionary for JordstykkeList from the JSON stream.
    /// </summary>
    /// <param name="matJsonStream"></param>
    /// <param name="ejerlavLookup"></param>
    /// <returns></returns>
    private static Dictionary<string, (string matrikelnummer, string ejerlavsnavn)> BuildJordstykkeLookup(
        Stream matJsonStream,
        Dictionary<string, string> ejerlavLookup)
    {
        var lookup = new Dictionary<string, (string, string)>();
        using var sr = new StreamReader(matJsonStream);
        using var reader = new JsonTextReader(sr);
        var today = DateTime.Now.Date;
        Console.Write("\rSearching...JordstykkeList");
        while (reader.Read())
        {
            if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "JordstykkeList")
            {
                reader.Read(); // Move to StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;
                Console.Write("\rLoading...JordstykkeList");
                int counter = 0;
                while (reader.Read() && reader.TokenType != JsonToken.EndArray)
                {
                    if (reader.TokenType == JsonToken.StartObject)
                    {
                        string? id = null, matrikelnummer = null, ejerlavLokalId = null;
                        DateTime? virkningFra = null, virkningTil = null, registreringFra = null, registreringTil = null;
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
                                    case "matrikelnummer":
                                        matrikelnummer = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    case "ejerlavLokalId":
                                        ejerlavLokalId = reader.TokenType == JsonToken.String ? (string)reader.Value : null;
                                        break;
                                    case "virkningFra":
                                        virkningFra = reader.TokenType == JsonToken.Date
                                            ? (DateTime?)reader.Value
                                            : DateTime.TryParse(reader.Value?.ToString(), out var vf) ? vf : null;
                                        break;
                                    case "virkningTil":
                                        virkningTil = reader.TokenType == JsonToken.Date
                                            ? (DateTime?)reader.Value
                                            : DateTime.TryParse(reader.Value?.ToString(), out var vt) ? vt : null;
                                        break;
                                    case "registreringFra":
                                        registreringFra = reader.TokenType == JsonToken.Date
                                            ? (DateTime?)reader.Value
                                            : DateTime.TryParse(reader.Value?.ToString(), out var rf) ? rf : null;
                                        break;
                                    case "registreringTil":
                                        registreringTil = reader.TokenType == JsonToken.Date
                                            ? (DateTime?)reader.Value
                                            : DateTime.TryParse(reader.Value?.ToString(), out var rt) ? rt : null;
                                        break;
                                    default:
                                        reader.Skip();
                                        break;
                                }
                            }
                        }
                        // Parse and filter dates
                        if (virkningFra == null || registreringFra == null)
                            continue;
                        if (virkningFra.Value.Date > today || registreringFra.Value.Date > today)
                            continue;
                        if (virkningTil != null || registreringTil != null)
                            continue;

                        if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(matrikelnummer) && !string.IsNullOrEmpty(ejerlavLokalId))
                        {
                            if (ejerlavLookup.TryGetValue(ejerlavLokalId, out var ejerlavsnavn))
                            {
                                lookup[id] = (matrikelnummer, ejerlavsnavn);
                                counter++;
                                if (counter % 1000 == 0)
                                    Console.Write($"\rLoading...JordstykkeList: {counter}");
                            }
                        }
                    }
                }
                Console.WriteLine($"\rDone loading JordstykkeList - {lookup.Count} items.");
                break;
            }
        }
        return lookup;
    }
}
