using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace ExternalSort
{
    public class MergeSort
    {
        private const int FileBufferSize = 32 * 1024;
        private readonly IComparer<string> _comparer;
        private long _inputFileLength = 0;

        public MergeSort(Settings settings)
        {
            Settings = settings;
            if (Settings.OrdinalStringSortOrder)
            {
                _comparer = StringComparer.InvariantCulture;
            }
            else
            {
                _comparer = new LineComparer();
            }
        }

        public Settings Settings { get; private set; }

        public void MergeSortFile(string inputFile, string outputFile)
        {
            var inputFileLength = new FileInfo(inputFile).Length;
            _inputFileLength = inputFileLength;

            var files = Split(inputFile, Path.GetTempPath());

            MergeSortFiles(files.Item2, files.Item1, outputFile, Comparator).Wait();

            foreach (var file in files.Item2)
            {
                File.Delete(file);
            }
        }

        private int Comparator(string x, string y)
        {
            return _comparer.Compare(x, y);
        }

        private async Task MergeSortFiles(IList<string> files, long totalLines, string outputFile, Comparison<string> linesEqualityComparer)
        {
            var maxQueueRecords = Settings.MaxQueueRecords;
            var sortedChunks = new SortedDictionary<string, List<AutoFileQueue>>(_comparer);

            var totalSize = 0L;
            foreach (var file in files)
            {
                var fileStreeam = OpenForAsyncRead(file);
                totalSize += fileStreeam.Length;

                var unzipStream = new GZipStream(fileStreeam, CompressionMode.Decompress);
                var reader = new StreamReader(unzipStream, Encoding.UTF8);
                var autoQueue = new AutoFileQueue(reader, maxQueueRecords);
                if (autoQueue.Any())
                {
                    var top = autoQueue.Peek();
                    if (sortedChunks.ContainsKey(top))
                    {
                        sortedChunks[top].Add(autoQueue);
                    }

                    sortedChunks.Add(top, new List<AutoFileQueue> { autoQueue });
                }
                else
                {
                    autoQueue.Dispose();
                    Debug.Assert(false, "Empty queue loaded from file. This should never happen!");
                }
            }

            using (new AutoStopwatch("Merge sort input", _inputFileLength))
            using (new AutoStopwatch("Merge sort compressed files ", totalSize))
            using (var sw = new StreamWriter(OpenForAsyncWrite(outputFile)))
            {
                var progress = 0;
                var totalWork = totalLines;

                while (sortedChunks.Any())
                {
                    Console.Write("{0:f2}%   \r", (100.0 * progress) / totalWork);

                    var lines = NWayMerge(sortedChunks);
                    foreach (var line in lines)
                    {
                        await sw.WriteLineAsync(line);
                    }

                    progress += lines.Count;
                }
            }
        }

        private async Task<string> LinesWriter(IList<string> lines, string tempPath)
        {
            var tempFileName = Path.Combine(tempPath, Path.GetRandomFileName());
            var fs = OpenForAsyncWrite(tempFileName);
            var gz = new GZipStream(fs, CompressionLevel.Fastest);
            using (var stringStream = new StreamWriter(gz, Encoding.UTF8, FileBufferSize))
            {
                foreach (var line in lines)
                {
                    await stringStream.WriteLineAsync(line);
                }
            }

            return tempFileName;
        }

        private Tuple<long, IList<string>> Split(string file, string tempLocationPath)
        {
            var files = new List<string>();
            var writeTask = Task.CompletedTask;
            var lineCount = 0L;

            using (new AutoStopwatch("Creating temp files", _inputFileLength))
            {
                using (var countedList = new CountedList(Settings.MaxMemoryUsageBytes / 2))
                {
                    countedList.MaxIntemsReached += (fullList, bytesCount) =>
                    {
                        writeTask.Wait();
                        writeTask = Task.Run(() =>
                        {
                            fullList.Sort(_comparer);
                            files.Add(LinesWriter(fullList, tempLocationPath).Result);
                        });
                    };

                    var bigInputFile = OpenForAsyncRead(file);
                    using (var inputStream = new StreamReader(bigInputFile))
                    {
                        var done = false;
                        while (!done)
                        {
                            var res = ReadLines(inputStream).Result;
                            var lines = res.Item1;
                            done = res.Item2;

                            lineCount += lines.Count;

                            foreach (var line in lines)
                            {
                                countedList.Add(line);
                            }
                        }
                    }
                }

                writeTask.Wait();
                if (lineCount > 0)
                {
                    var bytesPerLine = _inputFileLength / lineCount;
                    checked
                    {
                        Settings.MaxQueueRecords = (int)((long)Settings.MaxMemoryUsageBytes / bytesPerLine / files.Count / 4);
                    }
                }
            }

            Console.WriteLine("{0} files created. Total lines {1}. Max Queue size {2}", files.Count, lineCount, Settings.MaxQueueRecords);
            return new Tuple<long, IList<string>>(lineCount, files);
        }

        private async Task<Tuple<IList<string>, bool>> ReadLines(StreamReader reader, int maxCount = 1024)
        {
            var result = new List<string>();
            var done = false;
            for (int i = 0; i < maxCount; i++)
            {
                var line = await reader.ReadLineAsync();
                if (line != null)
                {
                    result.Add(line);
                }
                else
                {
                    done = true;
                }
            }

            return new Tuple<IList<string>, bool>(result, done);
        }

        private List<string> NWayMerge(IDictionary<string, List<AutoFileQueue>> sortedChunks, int maxCount = 1024)
        {
            var outList = new List<string>(maxCount);
            for (var i = 0; i < maxCount && sortedChunks.Any(); i++)
            {
                var top = sortedChunks.First();
                sortedChunks.Remove(top.Key);

                foreach (var topValueQeue in top.Value)
                {
                    outList.Add(topValueQeue.Dequeue());
                    if (!topValueQeue.Any())
                    {
                        topValueQeue.Dispose();
                        continue;
                    }

                    var newTop = topValueQeue.Peek();
                    if (sortedChunks.ContainsKey(newTop))
                    {
                        sortedChunks[newTop].Add(topValueQeue);
                    }
                    else
                    {
                        sortedChunks.Add(newTop, new List<AutoFileQueue> { topValueQeue });
                    }
                }

                /*
                 // ToDo: Add pick values from a current queue until closest key
                var afterTop = sortedChunks.FirstOrDefault();
                var queue = top.Value;

                if (!queue.Any())
                {
                    queue.Dispose();
                    continue;
                }

                var newKey = queue.Dequeue();
                while (queue.Any() && _comparer.Compare(newKey, afterTop.Key) < 0 && i + 1 < maxCount)
                {
                    outList.Add(newKey);
                    newKey = queue.Dequeue();
                    ++i;
                }

                sortedChunks.Add(newKey, queue);
                */
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
