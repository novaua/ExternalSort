using System;

namespace ExternalSort
{
    public class Settings
    {
        public Settings()
        {
            //1 GB
            MaxMemoryUsageBytes = 10 * 1024 * 1024;

            // Safe value
            MaxQueueRecords = 1000;

            OrdinalStringSortOrder = false;
        }

        public ulong MaxMemoryUsageBytes { get; set; }

        public int MaxQueueRecords { get; set; }

        public bool OrdinalStringSortOrder { get; set; }
    }
}
