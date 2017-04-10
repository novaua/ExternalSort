using System;

namespace ExternalSort
{
    public class Settings
    {
        public Settings()
        {
            //1 GB
            MaxMemoryUsageBytes = 1 * 1024 * 1024;

            MaxParallelWrites = Environment.ProcessorCount * 2;

            // Safe value
            MaxQueueRecords = 1000;

            OrdinalStringSortOrder = false;
        }

        public ulong MaxMemoryUsageBytes { get; set; }

        public int MaxParallelWrites { get; set; }

        public int MaxQueueRecords { get; set; }

        public long InputFileSize { get; set; }

        public long InputFileRecordCount { get; set; }

        public bool OrdinalStringSortOrder { get; set; }
    }
}
