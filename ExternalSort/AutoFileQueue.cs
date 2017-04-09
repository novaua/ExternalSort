using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
            if (_queueLoadTask != null)
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
                await LoadQueue(_queue,
                    _file,
                    _maxLoadedRecords
                    /*,
                    () =>
                    {
                        if (_fileOwner) _file.Close();
                    }*/);
            });
        }

        static async Task LoadQueue(Queue<string> queue, StreamReader file, int records)
        {
            for (var i = 0; i < records; i++)
            {
                if (file.Peek() < 0)
                {
                    //onDepleted?.Invoke();
                    break;
                }

                queue.Enqueue(await file.ReadLineAsync());
            }
        }
    }
}
