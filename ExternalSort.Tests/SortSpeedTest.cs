using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
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
                MaxDegreeOfParallelism = Environment.ProcessorCount - 1
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
                return x;
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
                File.WriteAllLines($"{lines}.sorted", lines);
            }
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
