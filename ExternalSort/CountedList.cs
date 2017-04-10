using System;
using System.Collections.Generic;

namespace ExternalSort
{
    public sealed class CountedList : List<string>
    {
        private readonly ulong _maxItems;

        public CountedList(ulong maxIntems)
        {
            _maxItems = maxIntems;
        }

        public Action<List<string>> MaxIntemsReached;

        public new void Add(string item)
        {
            base.Add(item);

            TotalItems += (ulong)item.Length;
            if (TotalItems >= _maxItems)
            {
                MaxIntemsReached?.Invoke(this);
            }
        }

        public ulong TotalItems { get; private set; }
    }
}
