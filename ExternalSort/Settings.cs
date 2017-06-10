using System;
using Common;

namespace ExternalSort
{
    public class Settings
    {
        public Settings()
        {
            //512 MB
            MaxMemoryUsageBytes = 1024 * 1024 * 1024;

            // Safe value
            MaxQueueRecords = 1000;

            MaxThreads = Environment.ProcessorCount;

            OrdinalStringSortOrder = false;

            DeflateTempFiles = true;

            MaxTempFileSize = 100L * Constants.Mb;
        }

        public ulong MaxMemoryUsageBytes { get; set; }

        public int MaxThreads { get; set; }

        public int MaxQueueRecords { get; set; }

        public bool OrdinalStringSortOrder { get; set; }

        public bool DeflateTempFiles { get; set; }

        public long MaxTempFileSize { get; set; }
    }
}
