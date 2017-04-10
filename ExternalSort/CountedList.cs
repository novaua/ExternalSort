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
            TotalItems = 0;
        }

        public Action<List<string>, ulong> MaxIntemsReached;

        public new void Add(string item)
        {
            base.Add(item);

            TotalItems += (ulong)item.Length;
            if (TotalItems >= _maxItems)
            {
                var clist = new List<string>(this);
                MaxIntemsReached?.Invoke(clist, TotalItems);

                Clear();
                TotalItems = 0;
            }
        }

        public ulong TotalItems { get; private set; }
    }
}
