using System;
using System.Configuration;
using System.Linq;

namespace ExternalSort
{
    public class Program
    {
        static void Main(string[] args)
        {
            var helpVariant = new[] { "/h", "-h", "--help" };
            if (args.Any() && helpVariant.Contains(args[0], StringComparer.OrdinalIgnoreCase))
            {
                Console.WriteLine("Big files sorting tool" + Environment.NewLine +
                                  "\tUsage ExternalSort <input file> <output file> [/ord[inal]]" + Environment.NewLine +
                                  "\tExample: " + Environment.NewLine +
                                  "\tExternalSort.exe in.txt outSorted.txt" + Environment.NewLine);
                return;
            }

            var imputFile = string.Empty;
            var outputFile = string.Empty;
            var option = string.Empty;

            if (args.Length >= 2)
            {
                imputFile = args[0];
                outputFile = args[1];
                if (args.Length == 3)
                {
                    option = args[2];
                }
            }
            else
            {
                Console.WriteLine("Incorrect arguments. Use /h for help.");
                return;
            }

            var appSettings = ConfigurationManager.AppSettings;
            var deflate = appSettings["DeflateTemp"];

            var ms = new MergeSort(
                new Settings
                {
                    OrdinalStringSortOrder = option.StartsWith("/ord"),
                    DeflateTempFiles = deflate == "true",
                });

            ms.MergeSortFile(imputFile, outputFile).Wait();
        }
    }
}
