using Common;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace FileCopy
{
    public class Program
    {
        private const int BufferSize = 8 * 1024 * 1024;
        private const int FileBufferSize = 64 * 1024;

        public static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("File copy utility" + Environment.NewLine +
                                  "\tUsage FileCopy <input file> <output path>" + Environment.NewLine +
                                  "\tExample: " + Environment.NewLine +
                                  "\tFileCopy.exe imput.txt d:\temp [/text]" + Environment.NewLine);
                return;
            }

            var inputFile = args[0];
            var outDir = args[1];
            var outIsFile = false;
            var outputFile = outIsFile ? outDir : Path.Combine(outDir, Path.GetFileName(inputFile));
            var asText = false;
            if (args.Length > 2)
            {
                asText = args[2].StartsWith("/text", StringComparison.InvariantCultureIgnoreCase);
            }
            else
            {
                Console.WriteLine($"Unknown option {args[3]}");
                return;
            }

            if (File.Exists(outputFile))
            {
                Console.WriteLine("File exist!. Overwrite Y/N?");
                var done = false;
                do
                {
                    var keyInf0 = Console.ReadKey();
                    var chStr = keyInf0.KeyChar.ToString().ToLower();
                    if (chStr == "y")
                    {
                        File.Delete(outputFile);
                        done = true;
                        outIsFile = true;
                    }
                    if (chStr == "n")
                    {
                        return;
                    }
                } while (!done);
            }


            if (!outIsFile && !Directory.Exists(outDir))
            {
                Directory.CreateDirectory(outDir);
            }

            var countBlocks = 0L;
            var totalWork = new FileInfo(inputFile).Length;
            var progress = 0L;
            using (var asw = new AutoStopwatch("File copy", totalWork))
            {
                try
                {
                    FileCopy(inputFile, outputFile, progressBytes =>
                    {
                        progress += progressBytes;
                        if (++countBlocks % 10 == 0)
                        {
                            var donePercent = progress / (totalWork * 1.0);
                            Console.Write($"\r {donePercent:P2}");
                        }
                    }).Wait();
                }
                catch (IOException e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }

        private static async Task FileCopy(string input, string output, Action<long> progress)
        {
            using (var inStream = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, FileBufferSize, true))
            {
                using (var outStream = new FileStream(output, FileMode.CreateNew, FileAccess.Write, FileShare.None, FileBufferSize, FileOptions.Asynchronous))
                {
                    var blocks = (inStream.Length / BufferSize) + (inStream.Length % BufferSize == 0 ? 0L : 1L);
                    var buffer = new byte[BufferSize];
                    for (var i = 0; i < blocks; ++i)
                    {
                        var read = await inStream.ReadAsync(buffer, 0, buffer.Length);
                        await outStream.WriteAsync(buffer, 0, read);
                        progress(read);
                    }
                }
            }
        }

        private static async Task FileCopyText(string input, string output, Action<long> progress)
        {
            using (var inStream = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, FileBufferSize, true))
            using (var inReader = new StreamReader(inStream))
            using (var outStream = new FileStream(output, FileMode.CreateNew, FileAccess.Write, FileShare.None, FileBufferSize, FileOptions.Asynchronous))
            using (var outWriter = new StreamWriter(outStream))
            {
                do
                {
                    var line = await inReader.ReadLineAsync();
                    if (line != null)
                    {
                        await outWriter.WriteLineAsync(line);
                    }
                    else
                    {
                        break;
                    }
                } while (true);
            }
        }
    }
}
