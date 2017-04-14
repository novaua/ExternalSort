using System;
using Common;

namespace ExternalSort
{
    public class Settings
    {
        public Settings()
        {
            //2 GB will work for 32 bit systems!
            MaxMemoryUsageBytes = 2 * Constants.GB;

            // Safe value
            MaxQueueRecords = 1000;

            OrdinalStringSortOrder = false;

            DeflateTempFiles = true;

            MaxProcessors = Environment.ProcessorCount;
        }

        public ulong MaxMemoryUsageBytes { get; set; }

        public int MaxQueueRecords { get; set; }

        public int MaxProcessors { get; set; }

        public bool OrdinalStringSortOrder { get; set; }

        public bool DeflateTempFiles { get; set; }
    }
}
