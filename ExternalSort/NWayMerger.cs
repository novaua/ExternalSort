using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ExternalSort
{
    public class NWayMerger: INwayMerger
    {
        public async Task MergeFilesAsync(IEnumerable<string> inputFiles, string sortedFile, Func<string, StreamReader> inputFileOpener, Func<string, Stream> outputFileOpener, Action<uint> lineProgress)
        {
            var chunks = MakeSortedChunks(inputFiles, inputFileOpener);
            await ChunkSorter(sortedFile, chunks, outputFileOpener, lineProgress);
        }

        public void MergeFiles(IEnumerable<string> inputFiles, string sortedFile, Func<string, StreamReader> inputFileOpener, Func<string, Stream> outputFileOpener, Action<uint> lineProgress)
        {
            MergeFilesAsync(inputFiles, sortedFile, inputFileOpener, outputFileOpener, lineProgress).Wait();
        }

        private async Task ChunkSorter(string outputFile,
            IDictionary<string, List<AutoFileQueue>> sortedChunks,
            Func<string, Stream> writeFileOpener,
            Action<uint> lineProgress)
        {
            uint linesCount = 0;
            using (var sw = new StreamWriter(writeFileOpener(outputFile)))
            {
                while (sortedChunks.Any())
                {
                    await NWayMerge(sortedChunks, async line =>
                    {
                        await sw.WriteLineAsync(line);
                        if (++linesCount == 1024)
                        {
                            lineProgress(linesCount);
                            linesCount = 0;
                        }
                    });
                }
            }

            lineProgress(linesCount);
        }

        private SortedDictionary<string, List<AutoFileQueue>> MakeSortedChunks(IEnumerable<string> sortedFiles, Func<string, StreamReader> fileOpener)
        {
            var sortedChunks = new SortedDictionary<string, List<AutoFileQueue>>();
            foreach (var file in sortedFiles)
            {
                var autoQueue = new AutoFileQueue(fileOpener(file), CancellationToken.None);
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

            return sortedChunks;
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

        private async Task NWayMerge(IDictionary<string, List<AutoFileQueue>> sortedChunks, Func<string, Task> lineWriter)
        {
            if (sortedChunks.Count == 1)
            {
                var singleQueue = sortedChunks.Values.First()[0];
                while (sortedChunks.Any())
                {
                    if (singleQueue.Any())
                    {
                        await lineWriter(singleQueue.Dequeue());
                    }
                    else
                    {
                        singleQueue.Dispose();
                        sortedChunks.Clear();
                    }
                }
            }

            while (sortedChunks.Any())
            {
                var top = sortedChunks.First();
                sortedChunks.Remove(top.Key);

                foreach (var topValueQeue in top.Value)
                {
                    await lineWriter(topValueQeue.Dequeue());
                    if (!topValueQeue.Any())
                    {
                        topValueQeue.Dispose();
                        continue;
                    }

                    AddToQueue(sortedChunks, topValueQeue);
                }
            }
        }  
    }
}
