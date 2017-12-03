using System;
using System.Collections.Generic;

namespace ExternalSort
{
    public interface IPriorityQueue<TKey, TValue> where TKey : IComparable<TKey>
    {
        int Count { get; }

        KeyValuePair<TKey, TValue> Peek();

        void Enqueue(TKey key, TValue value);

        KeyValuePair<TKey, TValue> Dequeue();
    }
}
