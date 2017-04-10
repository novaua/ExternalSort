using System;
using System.Globalization;

namespace Common
{
    public static class BytesFormatter
    {
        private const int KB = 1024;

        public static string Format(ulong bytes)
        {
            return Format((long)bytes);
        }

        // This function is used to format an integer representation of a file size (in bytes)
        // into a pretty string that includes file size metric.
        public static string Format(long bytes)
        {
            var orders = new[] { "TB", "GB", "MB", "KB", "bytes" };
            var order = orders.Length - 1;
            var printValue = (double)bytes;

            while (printValue >= KB && order > 0)
            {
                printValue /= KB;
                --order;
            }

            return $"{printValue:0.##} {orders[order]}";
        }
    }
}
