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
using System.Threading.Tasks.Dataflow;
using Common;

namespace ExternalSort
{
    public class MergeSort
    {
        private string CompleteMark = "Complete.Mark";
        private const int FileBufferSize = 128 * 1024;
        private readonly IComparer<string> _comparer;
        private long _inputFileLength = 0;
        private INwayMerger _merger;

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

            _merger = new NWayMerger();
        }

        public Settings Settings { get; private set; }

        public async Task MergeSortFile(string inputFile, string outputFile)
        {
            var inputFileLength = new FileInfo(inputFile).Length;
            _inputFileLength = inputFileLength;
            using (var asw = new AutoStopwatch("Total work", _inputFileLength))
            {
                var splitedFiles = new BufferBlock<string>();

                var producedFiles = 0;
                var spliter = Task.Run(() => SplitToFiles(inputFile, newFile =>
                {
                    splitedFiles.Post(newFile);
                    ++producedFiles;
                })).ContinueWith(a =>
                {
                    Console.WriteLine($"Total produced files: {producedFiles}");
                    splitedFiles.Complete();
                },
                    TaskContinuationOptions.AttachedToParent);

                var parallel = new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = Settings.MaxThreads
                };

                var iOparallel = new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 1
                };

                var reader = new TransformBlock<string, Tuple<string, string[]>>(file =>
                {
                    var lines = File.ReadAllLines(file);
                    return new Tuple<string, string[]>(file, lines);
                },
                 iOparallel);

                var sorter = new TransformBlock<Tuple<string, string[]>, Tuple<string, string[]>>(x =>
                {
                    Array.Sort(x.Item2);
                    return new Tuple<string, string[]>($"{x.Item1}.sorted", x.Item2);
                },
                parallel);

                var writer = new TransformBlock<Tuple<string, string[]>, string>(x =>
                {
                    File.WriteAllLines(x.Item1, x.Item2);
                    return x.Item1;
                },
                iOparallel);

                var pc = new DataflowLinkOptions { PropagateCompletion = true };

                splitedFiles.LinkTo(reader, pc);

                reader.LinkTo(sorter, pc);
                sorter.LinkTo(writer, pc);

                var maxBatchSize = 2;

                var sortedFilesFlow = new BatchBlock<string>(maxBatchSize);
                writer.LinkTo(sortedFilesFlow);

                var totalLinesWriten = 0L;
                var processedCount = 0;

                var filesMerger = new TransformBlock<string[], string>(x =>
                {
                    string newOutFile;
                    var parent = Path.GetDirectoryName(x[0]);

                    do
                    {
                        newOutFile = Path.Combine(parent, Path.GetRandomFileName());
                    } while (File.Exists(newOutFile));

                    var mrg = new NWayMerger();
                    mrg.MergeFiles(x, newOutFile, OpenForAsyncTextRead, OpenForAsyncWrite, p =>
                    {
                        Interlocked.Add(ref totalLinesWriten, p);
                    });

                    foreach (var file in x)
                    {
                        File.Delete(file);
                    }

                    Interlocked.Add(ref processedCount, x.Length - 1);
                    Console.WriteLine("{0} lines written to {1} from sources {2}", totalLinesWriten, newOutFile, string.Join(", ", x));
                    return newOutFile;
                });

                sortedFilesFlow.LinkTo(filesMerger, pc);

                var repeter = new ActionBlock<string>(file =>
                {
                    if (Interlocked.Add(ref processedCount, 0) > 1)
                    {
                        sortedFilesFlow.Post(file);
                    }
                    else
                    {
                        sortedFilesFlow.Complete();
                        File.Move(file, outputFile);
                    }
                });

                filesMerger.LinkTo(repeter, pc);

                await Task.WhenAll(spliter, writer.Completion, sorter.Completion, splitedFiles.Completion);

                Console.WriteLine($"Split to {producedFiles} files completed.");
                await Task.WhenAll(repeter.Completion, filesMerger.Completion, sortedFilesFlow.Completion);
            }
        }

        private void SplitToFiles(string bigFile, Action<string> onNewFile)
        {
            var maxSize = Settings.MaxTempFileSize;
            var tempRoot = Path.GetDirectoryName(Path.GetTempFileName());
            var mask = Path.Combine(tempRoot, "mergeSortTempFileNo_{0}");

            Algorithm.SplitTextFile(bigFile, mask, maxSize, onNewFile);
        }

        private int Comparator(string x, string y)
        {
            return _comparer.Compare(x, y);
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
