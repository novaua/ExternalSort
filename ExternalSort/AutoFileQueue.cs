using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ExternalSort
{
    public sealed class AutoFileQueue : IDisposable
    {
        private readonly BlockingCollection<string> _queue;
        private readonly StreamReader _file;

        private readonly bool _fileOwner;
        private bool _disposed;
        private Task _queueLoadTask;
        private CancellationTokenSource _cts;

        public AutoFileQueue(StreamReader file, CancellationToken ct, int maxLoadedRecords = 1024, bool fileOwner = true)
        {
            _file = file;
            _fileOwner = fileOwner;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

            _queue = new BlockingCollection<string>(maxLoadedRecords);

            // probe on non-empty
            var fl = _file.ReadLineAsync().Result;
            if (fl != null)
            {
                _queue.Add(fl);
                _queueLoadTask = Task.Run(async () =>
                {
                    while (true)
                    {
                        try
                        {
                            var line = await _file.ReadLineAsync();
                            if (line != null)
                            {
                                _queue.Add(line);
                            }
                            else
                            {
                                break;
                            }
                        }
                        catch (Exception e)
                        {
                            _cts.Cancel();
                            Console.WriteLine(e);
                            _queue.CompleteAdding();
                            throw;
                        }
                    }

                    _queue.CompleteAdding();
                });
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                WaitQueueLoaded();
                _queue.Dispose();

                if (_fileOwner)
                {
                    _file.Dispose();
                }

                _disposed = true;
            }
        }

        public string Peek()
        {
            return _queue.FirstOrDefault();
        }

        public string Dequeue()
        {
            return _queue.Take(_cts.Token);
        }

        public bool Any()
        {
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
    }
}
