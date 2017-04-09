using System;
using System.Collections.Generic;

namespace ExternalSort
{
    public interface IMergeSort
    {
        void MergeSortFile(string inputFile, string outputFile);

        void MergeSortFiles(IList<string> files, long totalLines, string file, Comparison<string> linesEqualityComparer);
    }
}
