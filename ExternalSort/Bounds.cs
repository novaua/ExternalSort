using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExternalSort
{
    public class Bounds
    {
        public Bounds()
        {
            //1 GB
            MaxMemoryUsageBytes = 1 * 1024 * 1024;

            MaxParallelWrites = Environment.ProcessorCount * 2;

            // Safe value
            MaxQueueRecords = 1000;
        }

        public ulong MaxMemoryUsageBytes { get; set; }

        public int MaxParallelWrites { get; set; }

        public int MaxQueueRecords { get; set; }


        public long InputFileSize { get; set; }

        public long InputFileRecordCount { get; set; }
    }
}
