using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DatafordelerConverter
{
    /// <summary>
    /// Provides methods to load common data from JSON files and build lookup dictionaries.
    /// </summary>
    public static class CommonDataLoader
    {
        /// <summary>
        /// Builds a lookup dictionary for NavngivenVejKommunedelList from the JSON file.
        /// </summary>
        /// <param name="jsonFile"></param>
        /// <returns></returns>
        public static Dictionary<string, (string kommune, string vejkode)> BuildNavngivenVejKommunedelLookup(string jsonFile)
        {
            Console.Write("\rSearching...NavngivenVejKommunedelList");
            var kommunedelLookup = new Dictionary<string, (string kommune, string vejkode)>();
            using var sr = new StreamReader(jsonFile);
            using var reader = new JsonTextReader(sr);
            while (reader.Read())
            {
                if (reader.TokenType != JsonToken.PropertyName ||
                    (string)reader.Value != "NavngivenVejKommunedelList") continue;
                reader.Read(); // StartArray
                if (reader.TokenType != JsonToken.StartArray)
                    break;

                Console.Write("\rLoading...NavngivenVejKommunedelList");
                var array = JArray.Load(reader);
                foreach (var item in array)
                {
                    var navngivenVej = (string?)item["navngivenVej"];
                    var kommune = (string?)item["kommune"];
                    var vejkode = (string?)item["vejkode"];
                    if (string.IsNullOrEmpty(navngivenVej) || kommune == null || vejkode == null) continue;
                    if (kommunedelLookup.ContainsKey(navngivenVej)) continue;
                    kommunedelLookup[navngivenVej] = (kommune, vejkode);
                }
                break;
            }
            Console.WriteLine($"\rDone loading navngivenVejKommunedelList - {kommunedelLookup.Count} items.");
            return kommunedelLookup;
        }

        /// <summary>
        /// Builds a lookup dictionary for PostnummerList from the JSON file.
        /// </summary>
        /// <param name="jsonFile"></param>
        /// <returns></returns>
        public static Dictionary<string, (string postnr, string navn)> BuildPostnummerLookup(string jsonFile)
        {
            Console.Write($"\rSearching...PostnummerList");
            var lookup = new Dictionary<string, (string postnr, string navn)>();
            using var sr = new StreamReader(jsonFile);
            using var reader = new JsonTextReader(sr);
            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.PropertyName && (string)reader.Value == "PostnummerList")
                {
                    Console.Write("\rLoading...PostnummerList");
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
    }
}
