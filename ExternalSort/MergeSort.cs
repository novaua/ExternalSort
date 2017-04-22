using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Common;

namespace ExternalSort
{
    public class MergeSort
    {
        private const int FileBufferSize = 128 * 1024;
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

        public async Task MergeSortFile(string inputFile, string outputFile)
        {
            var inputFileLength = new FileInfo(inputFile).Length;
            _inputFileLength = inputFileLength;
            var asw = new AutoStopwatch("Total work", _inputFileLength);
            var files = await Split(inputFile, Path.GetTempPath());
            try
            {
                await MergeSortFiles(files.Item2, files.Item1, outputFile, Comparator);
            }
            finally
            {
                asw.Dispose();

                foreach (var file in files.Item2)
                {
                    File.Delete(file);
                }
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
                totalSize += new FileInfo(file).Length;
                var reader = OpenForAsyncTextRead(file);
                var autoQueue = new AutoFileQueue(reader, CancellationToken.None, maxQueueRecords);
                if (autoQueue.Any())
                {
                    AddToQueue(sortedChunks, autoQueue);
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
                var writerTask = Task.CompletedTask;

                while (sortedChunks.Any())
                {
                    Console.Write("{0:f2}%   \r", (100.0 * progress) / totalWork);

                    var lines = NWayMerge(sortedChunks);
                    await writerTask;
                    writerTask = Task.Run(async () =>
                    {
                        foreach (var line in lines)
                        {
                            await sw.WriteLineAsync(line);
                        }
                    });

                    progress += lines.Count;
                }

                await writerTask;
            }
        }

        private async Task LinesWriter(IList<string> lines, string fileName)
        {
            using (var stringStream = OpenForAsyncTextWrite(fileName))
            {
                foreach (var line in lines)
                {
                    await stringStream.WriteLineAsync(line);
                }
            }
        }

        private void Sorter(BlockingCollection<Tuple<string, List<string>>> unsorted, BlockingCollection<Tuple<string, List<string>>> sorted)
        {
            foreach (var job in unsorted.GetConsumingEnumerable())
            {
                job.Item2.Sort(_comparer);
                sorted.Add(job);
            }
        }

        private async Task Writer(BlockingCollection<Tuple<string, List<string>>> sorted)
        {
            foreach (var fullList in sorted.GetConsumingEnumerable())
            {
                await LinesWriter(fullList.Item2, fullList.Item1);
            }
        }

        private Task[] SetupProcessing(BlockingCollection<Tuple<string, List<string>>> unsorted, BlockingCollection<Tuple<string, List<string>>> sorted)
        {
            var taskList = new List<Task>();
            for (var i = 0; i < Settings.MaxThreads; ++i)
            {
                taskList.Add(Task.Run(() => Sorter(unsorted, sorted)));
            }

            taskList.Add(Task.WhenAll(taskList.ToArray()).ContinueWith(a =>
            {
                sorted.CompleteAdding();
                if (a.IsFaulted)
                {
                    throw a.Exception;
                }
            }));

            for (var i = 0; i < 2; ++i)
            {
                taskList.Add(Task.Run(() => Writer(sorted)));
            }

            return taskList.ToArray();
        }

        private async Task<Tuple<long, IList<string>>> Split(string file, string tempLocationPath)
        {
            var files = new List<string>();
            var lineCount = 0L;

            using (var unsortedQueue = new BlockingCollection<Tuple<string, List<string>>>(Settings.MaxThreads))
            using (var sortedQueue = new BlockingCollection<Tuple<string, List<string>>>(Settings.MaxThreads))
            using (new AutoStopwatch("Creating temp files", _inputFileLength))
            {
                var sortAndWriteTasks = SetupProcessing(unsortedQueue, sortedQueue);

                using (var countedList = new CountedList(Settings.MaxMemoryUsageBytes / (uint)(2 * Settings.MaxThreads)))
                {
                    countedList.MaxIntemsReached += (fullList, bytesCount) =>
                    {
                        var tempFileName = Path.Combine(tempLocationPath, Path.GetRandomFileName());
                        var job = new Tuple<string, List<string>>(tempFileName, fullList);
                        unsortedQueue.Add(job);
                        files.Add(tempFileName);
                    };

                    var bigInputFile = OpenForAsyncRead(file);
                    using (var inputStream = new StreamReader(bigInputFile))
                    {
                        var done = false;
                        while (!done)
                        {
                            var res = await ReadLines(inputStream);
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

                unsortedQueue.CompleteAdding();
                Console.Write("Read complete");
                await Task.WhenAll(sortAndWriteTasks);

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

        private static void AddToQueue(IDictionary<string, List<AutoFileQueue>> sortedChunks, AutoFileQueue queue)
        {
            var newTop = queue.Peek();
            if (sortedChunks.ContainsKey(newTop))
            {
                sortedChunks[newTop].Add(queue);
            }
            else
            {
                sortedChunks.Add(newTop, new List<AutoFileQueue> { queue });
            }
        }

        private List<string> NWayMerge(IDictionary<string, List<AutoFileQueue>> sortedChunks, int maxCount = 1024)
        {
            var outList = new List<string>(maxCount);
            if (sortedChunks.Count == 1)
            {
                var singleQueue = sortedChunks.Values.First()[0];
                for (var i = 0; i < maxCount && sortedChunks.Any(); i++)
                {
                    if (singleQueue.Any())
                    {
                        outList.Add(singleQueue.Dequeue());
                    }
                    else
                    {
                        singleQueue.Dispose();
                        sortedChunks.Clear();
                    }
                }

                return outList;
            }

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

                    AddToQueue(sortedChunks, topValueQeue);
                }
            }

            return outList;
        }

        private StreamWriter OpenForAsyncTextWrite(string fileName)
        {
            var fileStream = OpenForAsyncWrite(fileName);
            return Settings.DeflateTempFiles
                ? new StreamWriter(new GZipStream(fileStream, CompressionMode.Compress), Encoding.UTF8)
                : new StreamWriter(fileStream, Encoding.UTF8);
        }

        private StreamReader OpenForAsyncTextRead(string fileName)
        {
            var fileStream = OpenForAsyncRead(fileName);
            return Settings.DeflateTempFiles
                ? new StreamReader(new GZipStream(fileStream, CompressionMode.Decompress), Encoding.UTF8)
                : new StreamReader(fileStream, Encoding.UTF8);
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
