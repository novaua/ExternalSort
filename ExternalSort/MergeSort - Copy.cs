using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Common;

namespace ExternalSort
{
    public class MergeSort : IMergeSort
    {
        private int FileBufferSize = 32 * 1024;
        public MergeSort(Bounds bounds)
        {
            Bounds = bounds;
        }

        public Bounds Bounds { get; private set; }

        public void MergeSortFile(string inputFile, string outputFile)
        {
            var files = Split(inputFile, Path.GetTempPath());

            SortFiles(files, Comparator);
            MergeSortFiles(files, outputFile, Comparator);

        }

        private int Comparator(string x, string y)
        {
            IdString left;
            IdString right;
            if (IdString.TryParseLine(x, out left) && IdString.TryParseLine(y, out right))
            {
                return left.CompareTo(right);
            }

            if (IdString.TryParseLine(y, out right))
            {
                return 1;
            }

            return string.Compare(x, y, StringComparison.InvariantCulture);
        }

        private void SortFiles(IList<string> files, Comparison<string> linesEqualityComparer)
        {
            foreach (var fileName in files)
            {
                var fileLines = File.ReadAllLines(fileName, Encoding.UTF8);
                Array.Sort(fileLines, linesEqualityComparer);
                File.WriteAllLines(fileName, fileLines, Encoding.UTF8);
            }
        }

        public void MergeSortFiles(IList<string> files, string outputFile, Comparison<string> linesEqualityComparer)
        {
            int chunks = files.Count;
            int recordsize = 100; // estimated record size
            int records = 10000000; // estimated total # records
            int maxusage = 500000000; // max memory usage
            int buffersize = maxusage / chunks; // bytes of each queue
            double recordoverhead = 7.5; // The overhead of using Queue<>
            int bufferlen = (int)(buffersize / recordsize / recordoverhead); // number of records in each queue

            // Open the files
            var readers = new StreamReader[chunks];
            for (int i = 0; i < chunks; i++)
            {
                readers[i] = new StreamReader(files[i]);
            }

            // Make the queues
            var queues = new Queue<string>[chunks];
            for (int i = 0; i < chunks; i++)
            {
                queues[i] = new Queue<string>(bufferlen);
                LoadQueue(queues[i], readers[i], bufferlen);
            }
            
            // Merge!
            var sw = new StreamWriter(outputFile);
            bool done = false;
            int lowest_index, j, progress = 0;
            string lowest_value;
            while (!done)
            {
                // Report the progress
                if (++progress % 5000 == 0)
                    Console.Write("{0:f2}%   \r",
                      100.0 * progress / records);

                // Find the chunk with the lowest value
                lowest_index = -1;
                lowest_value = "";
                for (j = 0; j < chunks; j++)
                {
                    if (queues[j] != null)
                    {
                        if (lowest_index < 0 ||
                          String.CompareOrdinal(
                            queues[j].Peek(), lowest_value) < 0)
                        {
                            lowest_index = j;
                            lowest_value = queues[j].Peek();
                        }
                    }
                }

                // Was nothing found in any queue? We must be done then.
                if (lowest_index == -1) { done = true; break; }

                // Output it
                sw.WriteLine(lowest_value);

                // Remove from queue
                queues[lowest_index].Dequeue();
                // Have we emptied the queue? Top it up
                if (queues[lowest_index].Count == 0)
                {
                    LoadQueue(queues[lowest_index],
                      readers[lowest_index], bufferlen);
                    // Was there nothing left to read?
                    if (queues[lowest_index].Count == 0)
                    {
                        queues[lowest_index] = null;
                    }
                }
            }
            sw.Close();

            // Close and delete the files
            for (int i = 0; i < chunks; i++)
            {
                readers[i].Close();
                File.Delete(files[i]);
            }
        }

        public IList<string> Split(string file, string tempLocationPath)
        {
            var files = new List<string>();
            using (var inputStream = new StreamReader(File.Open(file, FileMode.Open, FileAccess.Read, FileShare.Read)))
            {
                while (true)
                {
                    var tempFileName = Path.GetTempFileName();
                    files.Add(tempFileName);
                    Console.Write(".");
                    var line = inputStream.ReadLine();
                    using (var outputStream = new StreamWriter(File.Create(tempFileName, FileBufferSize, FileOptions.SequentialScan)))
                    {
                        using (var asw = new AutoStopwatch("Creating file", Bounds.MaxMemoryUsageBytes))
                        {
                            while (line != null && outputStream.BaseStream.Length < (long) Bounds.MaxMemoryUsageBytes)
                            {
                                outputStream.WriteLine(line);
                                line = inputStream.ReadLine();
                            }
                        }

                        Console.WriteLine("File created {0}", BytesFormatter.Format(outputStream.BaseStream.Length));
                    }

                    if (line == null)
                    {
                        break;
                    }
                }
            }

            Console.Write("{0} files created", files.Count);
            return files;
        }

        static void LoadQueue(Queue<string> queue, StreamReader file, int records)
        {
            for (var i = 0; i < records; i++)
            {
                if (file.Peek() < 0)
                {
                    break;
                }

                queue.Enqueue(file.ReadLine());
            }
        }

        IEnumerable<List<string>> ToLineChuncks(StreamReader reader, int chunkSize = 1024)
        {
            var done = false;
            while (!done)
            {
                var lb = new List<string>(chunkSize);
                for (var i = 0u; i < chunkSize && !done; ++i)
                {
                    var line = reader.ReadLine();
                    if (line != null)
                    {
                        lb.Add(line);
                    }
                    else
                    {
                        done = true;
                    }
                }

                if (lb.Any())
                {
                    yield return lb;
                }
            }
        }

        private class IdString : IComparable<IdString>
        {
            public long Id { get; set; }

            public string Alpha { get; set; }

            public int CompareTo(IdString other)
            {
                if (other == null)
                {
                    return 1;
                }

                var result = string.Compare(Alpha, other.Alpha, StringComparison.InvariantCulture);
                if (result == 0)
                {
                    result = Id.CompareTo(other.Id);
                }

                return result;
            }

            public static bool TryParseLine(string line, out IdString result)
            {
                result = null;
                var idAlpaStr = line.Split(new[] { '.', ' ' }, StringSplitOptions.RemoveEmptyEntries);
                if (idAlpaStr.Length == 2)
                {
                    long id;
                    if (long.TryParse(idAlpaStr[0], out id))
                    {
                        result = new IdString() { Id = id, Alpha = idAlpaStr[1] };
                        return true;
                    }
                }

                return false;
            }
        }
    }
}
