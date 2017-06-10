using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ExternalSort
{
    public interface INwayMerger
    {
        Task MergeFilesAsync(IEnumerable<string> inputFiles, string sortedFile, Func<string, StreamReader> inputFileOpener, Func<string, Stream> outputFileOpener, Action<uint> lineProgress);

        void MergeFiles(IEnumerable<string> inputFiles, string sortedFile, Func<string, StreamReader> inputFileOpener, Func<string, Stream> outputFileOpener, Action<uint> lineProgress);
    }
}
