using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DatafordelerConverter;

public static class MatDataLoader
{
    public static Dictionary<string, (string matrikelnummer, string ejerlavsnavn)> BuildMatrikelDict(string matJsonFile)
    {
        var ejerlavLookup = BuildEjerlavLookup(matJsonFile);
        return BuildJordstykkeLookup(matJsonFile, ejerlavLookup);
    }

    private static Dictionary<string, string> BuildEjerlavLookup(string matJsonFile)
    {
        var lookup = new Dictionary<string, string>();
        using var sr = new StreamReader(matJsonFile);
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

    private static Dictionary<string, (string matrikelnummer, string ejerlavsnavn)> BuildJordstykkeLookup(
        string matJsonFile,
        Dictionary<string, string> ejerlavLookup)
    {
        var lookup = new Dictionary<string, (string, string)>();
        using var sr = new StreamReader(matJsonFile);
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