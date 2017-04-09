using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExternalSort
{
    class Program
    {
        static void Main(string[] args)
        {
            var helpVariant = new[] { "/h", "-h", "--help" };
            if (args.Any() && helpVariant.Contains(args[0], StringComparer.OrdinalIgnoreCase))
            {
                Console.WriteLine("Big files sorting tool" + Environment.NewLine +
                                  "\tUsage ExternalSort <input file> <output file>" + Environment.NewLine +
                                  "\tExample: " + Environment.NewLine +
                                  "\tExternalSort.exe in.txt outSorted.txt" + Environment.NewLine);
                return;
            }

            var imputFile = string.Empty;
            var outputFile = string.Empty;

            if (args.Length == 2)
            {
                imputFile = args[0];
                outputFile = args[1];
            }
            else
            {
                Console.WriteLine("Incorrect arguments. Use /h for help.");
                return;
            }

            var ms = new MergeSort(new Bounds());
            ms.MergeSortFile(imputFile, outputFile);
        }
    }
}
