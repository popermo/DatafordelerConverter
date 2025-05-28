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
        /// Builds lookup dictionaries for both NavngivenVejKommunedelList and PostnummerList from a JSON stream in a single pass.
        /// </summary>
        /// <param name="jsonStream">The input JSON stream.</param>
        /// <returns>A tuple containing the NavngivenVejKommunedel and Postnummer lookup dictionaries.</returns>
        public static (
            Dictionary<string, (string kommune, string vejkode)> kommunedelLookup,
            Dictionary<string, (string postnr, string navn)> postnummerLookup
        ) BuildLookups(Stream jsonStream)
        {
            if (jsonStream == null || !jsonStream.CanRead)
            {
                throw new ArgumentException("The provided JSON stream is null or unreadable.", nameof(jsonStream));
            }

            Console.WriteLine("Starting lookup data processing...");
            var kommunedelLookup = new Dictionary<string, (string kommune, string vejkode)>();
            var postnummerLookup = new Dictionary<string, (string postnr, string navn)>();
            long processedItems = 0;
            long totalItems = 0; // Note: Total items may need to be estimated or precomputed if possible

            using var sr = new StreamReader(jsonStream);
            using var reader = new JsonTextReader(sr);
            string currentList = null;

            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.PropertyName)
                {
                    var propertyName = (string)reader.Value;
                    if (propertyName == "NavngivenVejKommunedelList" || propertyName == "PostnummerList")
                    {
                        currentList = propertyName;
                        Console.WriteLine($"\rLoading...{currentList}");
                        reader.Read(); // Move to StartArray
                        if (reader.TokenType != JsonToken.StartArray)
                        {
                            Console.WriteLine($"Error: Expected StartArray for {currentList}, found {reader.TokenType}");
                            continue;
                        }
                    }
                }
                else if (reader.TokenType == JsonToken.StartObject && currentList != null)
                {
                    // Process each object in the array incrementally
                    var item = JObject.Load(reader); // Load one object at a time
                    processedItems++;

                    if (currentList == "NavngivenVejKommunedelList")
                    {
                        var navngivenVej = (string?)item["navngivenVej"];
                        var kommune = (string?)item["kommune"];
                        var vejkode = (string?)item["vejkode"];
                        if (!string.IsNullOrEmpty(navngivenVej) && kommune != null && vejkode != null && !kommunedelLookup.ContainsKey(navngivenVej))
                        {
                            kommunedelLookup[navngivenVej] = (kommune, vejkode);
                        }
                    }
                    else if (currentList == "PostnummerList")
                    {
                        var id = (string?)item["id_lokalId"];
                        var postnr = (string?)item["postnr"];
                        var navn = (string?)item["navn"];
                        if (!string.IsNullOrEmpty(id))
                        {
                            postnummerLookup[id] = (postnr ?? "", navn ?? "");
                        }
                    }

                    // Update progress bar every 1000 items to reduce console overhead
                    if (processedItems % 1000 == 0)
                    {
                        DisplayProgressBar(processedItems, totalItems, currentList);
                    }
                }
            }

            // Final progress update
            DisplayProgressBar(processedItems, totalItems, currentList ?? "Lookups");
            Console.WriteLine($"\rDone loading lookups - NavngivenVejKommunedelList: {kommunedelLookup.Count} items, PostnummerList: {postnummerLookup.Count} items.");
            return (kommunedelLookup, postnummerLookup);
        }

        /// <summary>
        /// Displays a progress bar in the console.
        /// </summary>
        /// <param name="processedItems">Number of items processed.</param>
        /// <param name="totalItems">Total number of items (0 if unknown).</param>
        /// <param name="taskName">Name of the task being processed.</param>
        private static void DisplayProgressBar(long processedItems, long totalItems, string taskName)
        {
            int progressWidth = 50; // Width of the progress bar
            string progressInfo;

            if (totalItems > 0)
            {
                double progressPercentage = (double)processedItems / totalItems;
                int filledWidth = (int)(progressPercentage * progressWidth);
                progressInfo = $"[{new string('#', filledWidth)}{new string('-', progressWidth - filledWidth)}] {processedItems}/{totalItems} ({progressPercentage:P0})";
            }
            else
            {
                // If totalItems is unknown, show only processed items
                progressInfo = $"[{new string('#', progressWidth)}] {processedItems} items processed";
            }

            Console.Write($"\r{taskName}: {progressInfo}");
        }
    }
}