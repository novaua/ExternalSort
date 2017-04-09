using System;
using System.Globalization;

namespace Common
{
    public static class BytesFormatter
    {
        public static string Format(ulong bytes)
        {
            return Format((long)bytes);
        }

        // This function can be used to format an integer representation of a file size (in bytes)
        // into a pretty string that includes file size metric.
        public static string Format(long bytes)
        {
            const int Scale = 1024;
            var orders = new[] { "TB", "GB", "MB", "KB", "Bytes" };

            var max = (long)Math.Pow(Scale, orders.Length - 1);

            foreach (string order in orders)
            {
                if (bytes > max)
                {
                    return string.Format(CultureInfo.InvariantCulture, "{0:##.##} {1}", decimal.Divide(bytes, max), order);
                }

                max /= Scale;
            }

            return "0 Bytes";
        }
    }
}
