using System;
using System.ComponentModel;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Common;

namespace ExternalSort.Tests
{
    [TestClass]
    public class SortSpeedTest
    {
        [TestMethod]
        public void SingleTread_Test()
        {
            var toSorFileInfo = GetFilesInfo();

            using (var asw = new AutoStopwatch("Average speed", toSorFileInfo.Sum(x => x.Length)))
            {
                foreach (var file in toSorFileInfo.Select(x => x.FullName))
                {
                    SortFile(file);
                }
            }
        }

        [TestMethod]
        public void Dataflow_Test()
        {
            var toSorFileInfo = GetFilesInfo();
            var parallel = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount
            };

            var iOparallel = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 2
            };

            var reader = new TransformBlock<string, Tuple<string, string[]>>(inputFile =>
             {
                 var lines = File.ReadAllLines(inputFile);
                 return new Tuple<string, string[]>(inputFile, lines);
             },
             iOparallel);

            var sorter = new TransformBlock<Tuple<string, string[]>, Tuple<string, string[]>>(x =>
            {
                Array.Sort(x.Item2);
                return new Tuple<string, string[]>($"{x.Item1}.sorted", x.Item2);
            },
            parallel);

            var writer = new ActionBlock<Tuple<string, string[]>>(x =>
            {
                File.WriteAllLines(x.Item1, x.Item2);
            },
            iOparallel);

            var pc = new DataflowLinkOptions { PropagateCompletion = true };
            reader.LinkTo(sorter, pc);
            sorter.LinkTo(writer, pc);

            using (var asw = new AutoStopwatch("Average speed", toSorFileInfo.Sum(x => x.Length)))
            {
                foreach (var fi in toSorFileInfo)
                {
                    reader.Post(fi.FullName);
                }

                reader.Complete();
                Task.WaitAll(sorter.Completion, writer.Completion);
            }
        }

        [TestMethod]
        public void SortParallel_Test()
        {
            var toSorFileInfo = GetFilesInfo();
            var filesArray = toSorFileInfo.Select(x => x.FullName).ToArray();
            using (var asw = new AutoStopwatch("Average speed", toSorFileInfo.Sum(x => x.Length)))
            {
                Parallel.ForEach(filesArray, SortFile);
            }
        }

        [TestMethod]
        public void DataflowFull_Test()
        {
            var toSorFileInfo = GetFilesInfo();
            var parallel = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount
            };

            var iOparallel = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 2
            };

            var spliter = new TransformBlock<Tuple<string, long>, BufferBlock<string>>(bigInputFile =>
            {
                var maxSize = bigInputFile.Item2;
                var buffer = new byte[64 * 1024];
                var blocksCount = maxSize / buffer.Length;
                var inputFileSize = new FileInfo(bigInputFile.Item1).Length;
                var filesCount = (inputFileSize / maxSize) + (inputFileSize % maxSize == 0 ? 0 : 1);

                using (var file = File.OpenRead(bigInputFile.Item1))
                {
                    for (int i = 0; i < filesCount; i++)
                    {
                        using (var destFile = File.OpenWrite($"{bigInputFile.Item1}.{i}.split"))
                        {
                            bool eof = false;
                            for (int j = 0; j < blocksCount - 1; j++)
                            {
                                var rc = file.Read(buffer, 0, buffer.Length);
                                if (rc == 0)
                                {
                                    eof = true;
                                }
                                else
                                {
                                    destFile.Write(buffer, 0, rc);
                                }
                            }

                            if (!eof)
                            {
                                var rc = file.Read(buffer, 0, buffer.Length);
                                buffer.LastOrDefault()
                            }
                        }
                    }

                    var output = new BufferBlock<string>();

                    return output;
                }
            });

            var reader = new TransformBlock<string, Tuple<string, string[]>>(inputFile =>
                {
                    var lines = File.ReadAllLines(inputFile);
                    return new Tuple<string, string[]>(inputFile, lines);
                },
                iOparallel);

            var sorter = new TransformBlock<Tuple<string, string[]>, Tuple<string, string[]>>(x =>
                {
                    Array.Sort(x.Item2);
                    return new Tuple<string, string[]>($"{x.Item1}.sorted", x.Item2);
                },
                parallel);

            var writer = new ActionBlock<Tuple<string, string[]>>(x =>
                {
                    File.WriteAllLines(x.Item1, x.Item2);
                },
                iOparallel);

            var pc = new DataflowLinkOptions { PropagateCompletion = true };
            reader.LinkTo(sorter, pc);
            sorter.LinkTo(writer, pc);

            using (var asw = new AutoStopwatch("Average speed", toSorFileInfo.Sum(x => x.Length)))
            {
                foreach (var fi in toSorFileInfo)
                {
                    reader.Post(fi.FullName);
                }

                reader.Complete();
                Task.WaitAll(sorter.Completion, writer.Completion);
            }
        }

        private void SortFile(string inputFile)
        {
            var fi = new FileInfo(inputFile);
            if (!fi.Exists)
            {
                throw new ArgumentException($"File '{inputFile}' does not exist!");
            }

            using (var asw = new AutoStopwatch($"Sorting file '{inputFile}' {BytesFormatter.Format(fi.Length)} ", fi.Length))
            {
                var lines = File.ReadAllLines(inputFile);
                Array.Sort(lines);
                File.WriteAllLines($"{inputFile}.sorted", lines);
            }
        }

        private Tuple<byte[], byte[]> EndLineSplit(byte[] buffer)
        {
            var endl = Environment.NewLine;
            var endlBytes = Encoding.UTF8.GetBytes(endl);
            var found = -1;
            for (int i = 0; i != buffer.Length; i++)
            {
                var matchCount = 0;
                for (var j = 0; j != endlBytes.Length; ++j)
                {
                    if (buffer[i] == endlBytes[j])
                    {
                        ++matchCount;
                    }
                    else
                    {
                        matchCount = 0;
                    }
                }

                if (matchCount == endlBytes.Length)
                {
                    found = i - matchCount;
                    break;
                }
            }

            if (found >= 0)
            {
                if (found + endlBytes.Length == buffer.Length)
                {
                    return new Tuple<byte[], byte[]>(buffer, null);
                }

                var head = new byte[found + endlBytes.Length];
                Array.Copy(buffer, 0, head, 0, head.Length);
                var tail = new byte[buffer.Length - head.Length];

                Array.Copy(buffer, found + endlBytes.Length, tail, 0, tail.Length);

                return new Tuple<byte[], byte[]>(head, tail);
            }

            throw new ArgumentException("Unable to find end of line");
        }

        private static IOrderedEnumerable<FileInfo> GetFilesInfo()
        {
            var inputDir = @"..\..\..\bin\Debug";
            var files = Directory.EnumerateFiles(inputDir);
            var toSorFileInfo = files.Where(x => Path.GetFileName(x).StartsWith("input") && x.EndsWith(".txt"))
                .Select(x => new FileInfo(x))
                .OrderBy(x => x.Length);
            return toSorFileInfo;
        }
    }
}
