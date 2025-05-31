using System.Text;

namespace DatafordelerConverter;

public static class PostCode
{
    public static void ExportPostnummerToCsv(Stream outputStream, Dictionary<string, (string postnr, string navn)> postnummerLookup)
    {
        Console.Write("\rWriting PostCode.csv");
        using var sw = new StreamWriter(outputStream, Encoding.UTF8, 65536, leaveOpen: true);
        sw.WriteLine("PostalCode;;PostalDistrictName");

        int validCount = 0;
        foreach (var (postnr, navn) in postnummerLookup.Values)
        {
            if (!string.IsNullOrEmpty(postnr) && !string.IsNullOrEmpty(navn))
            {
                sw.WriteLine($"{postnr};;{navn}");
                validCount++;
            }
        }

        sw.Flush();
        Console.WriteLine($"\rDone writing PostCode.csv - {validCount} items");
    }
}
