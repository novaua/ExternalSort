using System;
using System.Collections.Generic;
using System.Linq;

namespace ExternalSort
{
    public sealed class CountedList : IDisposable
    {
        private const int DefaultCapacity = 1024;
        private List<string> _innerList = new List<string>(DefaultCapacity);
        private readonly ulong _maxItems;

        public CountedList(ulong maxIntems)
        {
            _maxItems = maxIntems;
            TotalItems = 0;
        }

        public Action<List<string>, ulong> MaxIntemsReached;

        public void Add(string item)
        {
            _innerList.Add(item);

            TotalItems += (ulong)item.Length;
            if (TotalItems >= _maxItems)
            {
                MaxIntemsReached?.Invoke(_innerList, TotalItems);
                _innerList = new List<string>(DefaultCapacity);
                TotalItems = 0;
            }
        }

        public ulong TotalItems { get; private set; }

        public void Dispose()
        {
            if (_innerList.Any())
            {
                MaxIntemsReached?.Invoke(_innerList, TotalItems);
                _innerList.Clear();
            }
        }
    }
}
