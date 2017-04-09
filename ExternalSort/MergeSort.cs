using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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

            SortFiles(files.Item2, Comparator);
            MergeSortFiles(files.Item2, files.Item1, outputFile, Comparator);

            foreach (var file in files.Item2)
            {
                File.Delete(file);
            }
        }

        private int Comparator(string x, string y)
        {
            /*
            IdString left;
            IdString right;
            if (IdString.TryMakeIdString(x, out left) && IdString.TryMakeIdString(y, out right))
            {
                return left.CompareTo(right);
            }
            */
            return string.Compare(x, y, StringComparison.InvariantCulture);
        }

        private void SortFiles(IList<string> files, Comparison<string> linesEqualityComparer)
        {
            foreach (var fileName in files)
            {
                using (new AutoStopwatch("Sorting file", new FileInfo(fileName).Length))
                {
                    var fileLines = File.ReadAllLines(fileName);
                    Array.Sort(fileLines, linesEqualityComparer);
                    File.WriteAllLines(fileName, fileLines, Encoding.UTF8);
                }
            }
        }

        public void MergeSortFiles(IList<string> files, long totalLines, string outputFile, Comparison<string> linesEqualityComparer)
        {
            if (files.Count == 1)
            {
                File.Move(files[0], outputFile);
                return;
            }

            int maxQueueRecords = Bounds.MaxQueueRecords;
            var sortedChunks = new SortedDictionary<string, AutoFileQueue>();

            var totalSize = 0L;
            // Open the files

            foreach (var file in files)
            {
                var reader = new StreamReader(File.OpenRead(file), Encoding.UTF8);
                
                var autoQueue = new AutoFileQueue(reader, maxQueueRecords);
                var top = autoQueue.Dequeue();
                sortedChunks.Add(top, autoQueue);

                totalSize += reader.BaseStream.Length;
            }

            using (new AutoStopwatch("Merge sorted files ", totalSize))
            using (var sw = new StreamWriter(outputFile))
            {
                int progress = 0;

                while (sortedChunks.Any())
                {
                    if (++progress % 1000 == 0)
                    {
                        Console.Write("{0:f2}%   \r", (100.0 * progress) / totalLines);
                    }

                    foreach (var line in NWayMerge(sortedChunks))
                    {
                        ++progress;
                        sw.WriteLine(line);
                    }
                }
            }
        }

        public Tuple<long, IList<string>> Split(string file, string tempLocationPath)
        {
            var bigInputFile = File.Open(file, FileMode.Open, FileAccess.Read, FileShare.Read);

            var files = new List<string>();
            var tempFileName = Path.GetTempFileName();
            files.Add(tempFileName);

            var outputStream = new StreamWriter(tempFileName, false, Encoding.UTF8, FileBufferSize);
            var asw = new AutoStopwatch("Creating file", (long)Bounds.MaxMemoryUsageBytes);
            var lineCount = 0L;
            try
            {
                using (var inputStream = new StreamReader(bigInputFile))
                {
                    foreach (var chunks in ToLineChuncks(inputStream))
                    {
                        lineCount += chunks.Count;
                        foreach (var line in chunks)
                        {
                            outputStream.WriteLine(line);
                        }

                        if (outputStream.BaseStream.Length >= (long)Bounds.MaxMemoryUsageBytes && inputStream.Peek() >= 0)
                        {
                            outputStream.Dispose();
                            tempFileName = Path.GetTempFileName();
                            files.Add(tempFileName);
                            outputStream = new StreamWriter(tempFileName, false, Encoding.UTF8, FileBufferSize);
                            asw.Dispose();
                            asw = new AutoStopwatch("Creating file", (long)Bounds.MaxMemoryUsageBytes);
                        }
                    }

                    if (lineCount > 0)
                    {
                        var bytesPerLine = bigInputFile.Length / lineCount;
                        checked
                        {
                            Bounds.MaxQueueRecords = (int)((long)Bounds.MaxMemoryUsageBytes / bytesPerLine / files.Count / 4);
                        }
                    }
                }
            }
            finally
            {
                outputStream.Dispose();
                asw.Dispose();
            }

            Console.WriteLine("{0} files created. Total lines {1}", files.Count, lineCount);
            return new Tuple<long, IList<string>>(lineCount, files);
        }

        static async Task LoadQueue(Queue<string> queue, StreamReader file, int records)
        {
            for (var i = 0; i < records; i++)
            {
                if (file.Peek() < 0)
                {
                    break;
                }

                queue.Enqueue(await file.ReadLineAsync());
            }
        }

        private IEnumerable<List<string>> ToLineChuncks(StreamReader reader, int chunkSize = 1024)
        {
            var done = false;
            while (!done)
            {
                var lb = new List<string>();
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

        private List<string> NWayMerge(IDictionary<string, AutoFileQueue> sortedChunks, int maxCount = 1024)
        {
            var outList = new List<string>(maxCount);
            if (sortedChunks.Count == 1)
            {
                var theOnly = sortedChunks.First();
                var queue = theOnly.Value;

                outList.Add(theOnly.Key);

                for (int i = 1; i < maxCount && queue.Any() && sortedChunks.Any(); i++)
                {
                    outList.Add(queue.Dequeue());
                }

                queue.Dispose();
            }

            for (var i = 0; i < maxCount && sortedChunks.Any(); i++)
            {
                var top = sortedChunks.First();
                
                outList.Add(top.Key);
                sortedChunks.Remove(top.Key);

                var afterTop = sortedChunks.FirstOrDefault();
                var queue = top.Value;

                if (!queue.Any())
                {
                    queue.Dispose();
                    continue;
                }

                var newKey = queue.Dequeue();
                while (queue.Any() && newKey.CompareTo(afterTop.Key) < 0 && i + 1 < maxCount)
                {
                    outList.Add(newKey);
                    newKey = queue.Dequeue();
                    ++i;
                }
                
                sortedChunks.Add(newKey, queue);
            }

            return outList;
        }
    }
}
