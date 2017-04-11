using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Common;

namespace DataGenerator
{
    public class Program
    {
        private const uint Mb = 1024 * 1024;
        private const uint Gb = 1024 * Mb;

        private const ulong DafaultSize = Mb;
        private const int OutFileBuffer = 128 * 1024;
        private const int ChunkSize = 128;
        private const int MaxWordsInLine = 4;

        private const string DafaultOutput = "out.txt";

        static void Main(string[] args)
        {
            var helpVariant = new[] { "/h", "-h", "--help" };
            if (args.Any() && helpVariant.Contains(args[0], StringComparer.OrdinalIgnoreCase))
            {
                Console.WriteLine("Test file generator" + Environment.NewLine +
                                  "\tUsage DataGenerator [size[MB|GB]] [output file]" + Environment.NewLine +
                                  "\t" + string.Format(CultureInfo.InvariantCulture, "Default size: {0} bytes, file: '{1}'", DafaultSize, DafaultOutput) + Environment.NewLine +
                                  "\tExample: " + Environment.NewLine +
                                  "\tDataGenerator.exe 10MB table.txt" + Environment.NewLine +
                                  "\tGenerates 10 MB file" + Environment.NewLine);
                return;
            }

            string outFile = DafaultOutput;
            ulong outSize = DafaultSize;

            if (args.Length >= 1)
            {
                uint size;
                var intPart = Regex.Match(args[0], @"\d+").Value;
                if (intPart.Any() && uint.TryParse(intPart, out size))
                {
                    var lowArg = args[0].ToLowerInvariant();
                    if (lowArg.EndsWith("mb"))
                    {
                        outSize = (ulong)size * Mb;
                    }
                    else if (lowArg.EndsWith("gb"))
                    {
                        outSize = (ulong)size * Gb;
                    }
                    else
                    {
                        outSize = size;
                    }
                }
                else
                {
                    Console.WriteLine("Provide file size. Use /h for help.");
                    return;
                }

                if (args.Length == 2)
                {
                    outFile = args[1];
                }
                else
                {
                    Console.WriteLine("Too many arguments. Use /h for help.");
                    return;
                }
            }

            Console.WriteLine("Generation file {0}", BytesFormatter.Format(outSize));

            var appSettings = ConfigurationManager.AppSettings;
            var fullDictionary = File.ReadAllLines(appSettings["WordsFile"]);
            GenerateFile(fullDictionary, outFile, outSize).Wait();
        }

        private static async Task GenerateFile(string[] dictionary, string fileName, ulong maxSize)
        {
            var rand = new Random();
            ulong lineCount = 1;

            using (var file = File.Create(fileName, OutFileBuffer, FileOptions.Asynchronous | FileOptions.SequentialScan))
            using (var sw = new StreamWriter(file, Encoding.UTF8, OutFileBuffer, true))
            using (var stopWatch = new AutoStopwatch("Test file generation"))
            {
                while (file.Length < (long)maxSize)
                {
                    var lines = GenerateLines(dictionary, rand, lineCount, ChunkSize);
                    await sw.WriteAsync(lines);
                    lineCount += ChunkSize;
                }

                stopWatch.WorkAmount = file.Length;
                Console.WriteLine("File '{0}' of total {1} lines of size {2} generated", fileName, lineCount, BytesFormatter.Format(file.Length));
            }
        }

        private static string GenerateLines(string[] dictionary, Random rand, ulong id, int chunkCount)
        {
            var wordsCount = dictionary.Length - 1;
            var sb = new StringBuilder();
            var firstLine = string.Empty;
            for (var i = 0u; i < chunkCount - 1; ++i)
            {
                var wordsList = new List<string>();
                for (var j = 0; j < rand.Next(1, MaxWordsInLine + 1); ++j)
                {
                    var nextWordId = rand.Next() % wordsCount;
                    wordsList.Add(dictionary[nextWordId]);
                }

                var worsdLine = string.Join(" ", wordsList);
                var nextId = id + i;
                sb.AppendLine($"{nextId}. {worsdLine}");
                if (string.IsNullOrEmpty(firstLine))
                {
                    firstLine = sb.ToString();
                }
            }

            // assure repetitions
            sb.Append(firstLine);

            return sb.ToString();
        }
    }
}
