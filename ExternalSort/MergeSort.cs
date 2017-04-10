using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace ExternalSort
{
    public class MergeSort : IMergeSort
    {
        private const int FileBufferSize = 32 * 1024;

        public MergeSort(Bounds bounds)
        {
            Bounds = bounds;
        }

        public Bounds Bounds { get; private set; }

        public void MergeSortFile(string inputFile, string outputFile)
        {
            var files = Split(inputFile, Path.GetTempPath()).Result;

            MergeSortFiles(files.Item2, files.Item1, outputFile, Comparator).Wait();

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

        public async Task MergeSortFiles(IList<string> files, long totalLines, string outputFile, Comparison<string> linesEqualityComparer)
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
                var unzipStream = new GZipStream(OpenForAsyncRead(file), CompressionMode.Decompress);
                var reader = new StreamReader(unzipStream, Encoding.UTF8);

                using (var autoQueue = new AutoFileQueue(reader, maxQueueRecords))
                {
                    var top = autoQueue.Dequeue();
                    sortedChunks.Add(top, autoQueue);

                    totalSize += reader.BaseStream.Length;
                }
            }

            using (new AutoStopwatch("Merge sorted files ", totalSize))
            using (var sw = new StreamWriter(OpenForAsyncWrite(outputFile)))
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
                        await sw.WriteLineAsync(line);
                    }
                }
            }
        }

        public async Task<string> LinesWriter(IList<string> lines)
        {
            var tempFileName = Path.GetTempFileName();

            var fs = OpenForAsyncWrite(tempFileName);
            var gz = new GZipStream(fs, CompressionMode.Compress);
            using (var stringStream = new StreamWriter(gz, Encoding.UTF8, FileBufferSize))
            {
                foreach (var line in lines)
                {
                    await stringStream.WriteLineAsync(line);
                }
            }

            return tempFileName;
        }

        public async Task<Tuple<long, IList<string>>> Split(string file, string tempLocationPath)
        {
            var files = new List<string>();

            var writeTask = Task.CompletedTask;
            var lineCount = 0L;
            var countedList = new CountedList(Bounds.MaxMemoryUsageBytes / 2);
            countedList.MaxIntemsReached += fullList =>
            {
                writeTask.Wait();
                var localList = countedList;
                lineCount += countedList.Count;

                writeTask =
                Task.Factory.StartNew(() => localList.Sort())
                .ContinueWith(async antecendent =>
                        {
                            using (new AutoStopwatch("Creating file", (long)Bounds.MaxMemoryUsageBytes))
                            {
                                files.Add(await LinesWriter(localList));
                                Console.WriteLine($"File written '{files.Last()}'");
                            }
                        }
                );

                countedList = new CountedList(Bounds.MaxMemoryUsageBytes / 2);
            };

            var bigInputFile = OpenForAsyncRead(file);
            using (var inputStream = new StreamReader(bigInputFile))
            {
                foreach (var line in await ReadLines(inputStream))
                {
                    countedList.Add(line);
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

            Console.WriteLine("{0} files created. Total lines {1}", files.Count, lineCount);
            return new Tuple<long, IList<string>>(lineCount, files);
        }

        private async Task<IList<string>> ReadLines(StreamReader reader, int maxCount = 1024)
        {
            var result = new List<string>();
            for (int i = 0; i < maxCount; i++)
            {
                result.Add(await reader.ReadLineAsync());
            }

            return result;
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

        private static FileStream OpenForAsyncWrite(string fileName)
        {
            return new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, FileBufferSize, FileOptions.Asynchronous);
        }

        private static FileStream OpenForAsyncRead(string fileName)
        {
            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, FileBufferSize, FileOptions.Asynchronous);
        }
    }
}
