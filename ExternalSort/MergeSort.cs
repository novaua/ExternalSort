using System;
using System.Collections.Generic;
using System.Configuration;
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

            var files = Split(inputFile, Path.GetTempPath());
            await MergeSortFiles(files.Item2, files.Item1, outputFile, Comparator);

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
                totalSize += new FileInfo(file).Length;
                var reader = OpenForAsyncTextRead(file);
                var autoQueue = new AutoFileQueue(reader, maxQueueRecords);
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

        private Tuple<long, IList<string>> Split(string file, string tempLocationPath)
        {
            var files = new List<string>();
            var lineCount = 0L;
            var maxProcessors = Settings.MaxProcessors;
            var maxFileSize = (long)Settings.MaxMemoryUsageBytes / maxProcessors;

            using (new AutoStopwatch($"Creating temp files using {maxProcessors} threads", _inputFileLength))
            {
                using (var bigInputFile = OpenForAsyncRead(file))
                {
                    var rangeList = TextLinesAwareSplit(bigInputFile, maxFileSize);
                    bigInputFile.Close();
                    MakeTempFiles(files, rangeList.Count, tempLocationPath);

                    Parallel.ForEach(rangeList,
                        new ParallelOptions { MaxDegreeOfParallelism = maxProcessors },
                        (range, state, id) =>
                    {
                        List<string> linesList;
                        using (var inputFile = new StreamReader(OpenForAsyncRead(file)))
                        {
                            linesList = ReadAllLinesInRange(inputFile, range).Result;
                            linesList.Sort(_comparer);
                        }

                        Interlocked.Add(ref lineCount, linesList.Count);
                        LinesWriter(linesList, files[(int)id]).Wait();
                    });
                }

                if (lineCount > 0)
                {
                    var bytesPerLine = _inputFileLength / lineCount;
                    checked
                    {
                        Settings.MaxQueueRecords = (int)((long)Settings.MaxMemoryUsageBytes / bytesPerLine / files.Count / 4);
                    }
                }
            }

            Console.WriteLine("{0} files created. Total lines {1}. Max Queue size {2}. Max file size {3}", files.Count, lineCount, Settings.MaxQueueRecords, maxFileSize);
            return new Tuple<long, IList<string>>(lineCount, files);
        }

        private async Task<List<string>> ReadAllLinesInRange(StreamReader reader, OffsetLength<long> range)
        {
            var result = new List<string>();
            var done = false;
            var endOffset = range.Offset + range.Length;

            while (reader.BaseStream.Position < endOffset)
            {
                var line = await reader.ReadLineAsync();
                Debug.Assert(line != null, "Sanity check");

                result.Add(line);
            }

            return new List<string>(result);
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

        private IList<OffsetLength<long>> TextLinesAwareSplit(Stream input, long maxChunkLenght)
        {
            var length = input.Length;
            var partsCount = length / maxChunkLenght + length % maxChunkLenght > 0 ? 1 : 0;
            var partList = new List<OffsetLength<long>>(partsCount);

            for (var i = 0L; i < length; i += maxChunkLenght)
            {
                var maxLength = Math.Min(maxChunkLenght, length - i);
                partList.Add(new OffsetLength<long> { Offset = i, Length = maxLength });
            }

            for (var j = 0; j < partList.Count - 1; ++j)
            {
                var current = partList[j];
                var next = partList[j + 1];

                var endOffset = current.Offset + current.Length;
                Debug.Assert(next.Offset == endOffset, "Sanity check");

                var offsetDelta = SeekForwardEolOffset(input, endOffset);
                if (offsetDelta != 0)
                {
                    current.Length += offsetDelta;
                    next.Offset += offsetDelta;
                    next.Length -= offsetDelta;
                }
            }

            return partList;
        }

        private long SeekForwardEolOffset(Stream input, long startPos)
        {
            var plusOffset = 0L;
            if (startPos >= input.Length)
            {
                throw new ArgumentException("Offset is out of file bounds!");
            }

            input.Seek(startPos, SeekOrigin.Begin);

            var buffer = new byte[256];
            var found = false;
            while (input.CanRead && !found)
            {
                var maxRead = (int)Math.Min(buffer.Length, input.Length - plusOffset);
                var readThisTime = input.Read(buffer, 0, maxRead);
                if (readThisTime <= 0)
                {
                    throw new IOException("Unexpected EOF reached");
                }

                var resultOffset = Algorithm.FindEolOffset(buffer);
                if (resultOffset >= 0)
                {
                    plusOffset += resultOffset;
                    found = true;
                }
                else
                {
                    plusOffset += readThisTime;
                }
            }

            return found ? plusOffset : -1;
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

        private static void MakeTempFiles(IList<string> destination, int count, string tempLocationPath)
        {
            destination.Clear();
            while (destination.Count < count)
            {
                var tempFileName = Path.Combine(tempLocationPath, Path.GetRandomFileName());
                if (File.Exists(tempFileName))
                {
                    continue;
                }

                destination.Add(tempFileName);
            }
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
    }
}
