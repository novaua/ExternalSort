using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ExternalSort
{
    public interface IMergeSort
    {
        void MergeSortFile(string inputFile, string outputFile);

        Task MergeSortFiles(IList<string> files, long totalLines, string file, Comparison<string> linesEqualityComparer);
    }
}
