using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace ExternalSort
{
    public sealed class AutoFileQueue : IDisposable
    {
        private readonly Queue<string> _queue = new Queue<string>();
        private readonly StreamReader _file;

        private readonly int _maxLoadedRecords;
        private readonly bool _fileOwner;
        private bool _disposed;
        private Task _queueLoadTask;

        public AutoFileQueue(StreamReader file, int maxLoadedRecords = 1024, bool fileOwner = true)
        {
            _file = file;
            _maxLoadedRecords = maxLoadedRecords;
            _fileOwner = fileOwner;

            StartLoadQueueFromFile();
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                if (_fileOwner)
                {
                    _file.Dispose();
                }

                _disposed = true;
            }
        }

        public string Dequeue()
        {
            WaitQueueLoaded();
            var result = _queue.Dequeue();
            if (!_queue.Any())
            {
                StartLoadQueueFromFile();
            }

            return result;
        }

        public bool Any()
        {
            WaitQueueLoaded();
            return _queue.Any();
        }

        private void WaitQueueLoaded()
        {
            if (_queueLoadTask != null && !_disposed)
            {
                _queueLoadTask.Wait();
                _queueLoadTask.Dispose();
                _queueLoadTask = null;
            }
        }

        private void StartLoadQueueFromFile()
        {
            Debug.Assert(_queueLoadTask == null, "Sanity check");
            _queueLoadTask = Task.Run(async () =>
            {
                var done = await LoadQueue(_queue,
                    _file,
                    _maxLoadedRecords);
                if (done)
                {
                    Dispose();
                }
            });
        }

        /// <summary>
        /// Feels the queue
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="file"></param>
        /// <param name="records"></param>
        /// <returns>true when done</returns>
        static async Task<bool> LoadQueue(Queue<string> queue, StreamReader file, int records)
        {
            for (var i = 0; i < records; i++)
            {
                var line = await file.ReadLineAsync();
                if (line != null)
                {
                    queue.Enqueue(line);
                }
                else
                {
                    return true;
                }
            }

            return false;
        }
    }
}
