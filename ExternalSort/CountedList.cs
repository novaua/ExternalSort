using System;
using System.Collections.Generic;

namespace ExternalSort
{
    /// <summary>
    /// Invokes MaxitemReached action passing tha current list and creates a new list
    /// </summary>
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

        public Action<List<string>, ulong> MaxIntemReached;

        public void Add(string item)
        {
            _innerList.Add(item);

            TotalItems += (ulong)item.Length;
            if (TotalItems >= _maxItems)
            {
                MaxIntemReached?.Invoke(_innerList, TotalItems);
                _innerList = new List<string>(DefaultCapacity);
                TotalItems = 0;
            }
        }

        public ulong TotalItems { get; private set; }

        public void Dispose()
        {
            if (TotalItems != 0)
            {
                MaxIntemReached?.Invoke(_innerList, TotalItems);
                TotalItems = 0;
            }
        }
    }
}
