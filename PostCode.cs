namespace DatafordelerConverter;

public static class PostCode
{
    /// <summary>
    /// Orchestrates the export of PostnummerList to CSV.
    /// </summary>
    public static void ExportPostnummerToCsv(string csvFile,
        Dictionary<string, (string postnr, string navn)> postnummerLookup)
    {
        ExportPostnummerListCsv(csvFile, postnummerLookup);
    }

    /// <summary>
    /// Writes the postnummerLookup dictionary to a CSV file.
    /// </summary>
    private static void ExportPostnummerListCsv(string csvFile,
        Dictionary<string, (string postnr, string navn)> postnummerLookup)
    {
        Console.WriteLine("\rWriting PostCode.csv");
        using var sw = new StreamWriter(csvFile, false, System.Text.Encoding.UTF8);
        sw.WriteLine("PostalCode;;PostalDistrictName");

        foreach (var kvp in postnummerLookup)
        {
            var (postnr, navn) = kvp.Value;
            if (!string.IsNullOrEmpty(postnr) && !string.IsNullOrEmpty(navn))
            {
                sw.WriteLine($"{postnr};;{navn}");
            }
        }

        sw.Flush();
        Console.WriteLine($"\rDone writing PostCode.csv - {postnummerLookup.Count} items");
    }
}