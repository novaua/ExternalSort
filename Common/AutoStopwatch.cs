using System;
using System.Diagnostics;
using System.Globalization;

namespace Common
{
    public sealed class AutoStopwatch : IDisposable
    {
        private readonly Stopwatch _stopwatch;
        private readonly string _actionName;

        public AutoStopwatch(string actionName = "Action", long workAmountBytes = 0)
        {
            WorkAmount = workAmountBytes;
            _actionName = actionName;

            _stopwatch = Stopwatch.StartNew();
        }

        public Action<TimeSpan, double, string> ReportAction { get; set; }

        public long WorkAmount { get; set; }

        public void Dispose()
        {
            _stopwatch.Stop();

            var time = _stopwatch.Elapsed;
            var bytesPerSecond = (WorkAmount > 0) ? (WorkAmount / time.TotalSeconds) : 0d;

            var msg = string.Format(CultureInfo.InvariantCulture,
                    "{0} of '{1}' took : {2:F2} s, speed {3}/s",
                    _actionName,
                    BytesFormatter.Format(WorkAmount),
                    time.TotalSeconds,
                    BytesFormatter.Format((ulong)bytesPerSecond));

            if (ReportAction != null)
            {
                ReportAction(time, bytesPerSecond, msg);
            }
            else
            {
                Console.WriteLine(msg);
            }
        }
    }
}
