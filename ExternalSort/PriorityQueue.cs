using System;
using System.Collections.Generic;
using System.Linq;

namespace ExternalSort
{
    public class PriorityQueue<TKey, TValue> where TKey : IComparable<TKey>
    {
        private readonly List<KeyValuePair<TKey, TValue>> _data;

        public PriorityQueue()
        {
            _data = new List<KeyValuePair<TKey, TValue>>();
        }

        public int Count => _data.Count;

        public KeyValuePair<TKey, TValue> Peek()
        {
            var frontItem = _data[0];
            return frontItem;
        }

        public void Enqueue(TKey key, TValue value)
        {
            _data.Add(new KeyValuePair<TKey, TValue>(key, value));
            var ci = _data.Count - 1;
            while (ci > 0)
            {
                var pi = (ci - 1) / 2;
                if (_data[ci].Key.CompareTo(_data[pi].Key) >= 0)
                {
                    break;
                }

                Swap(ci, pi);
                ci = pi;
            }
        }

        public KeyValuePair<TKey, TValue> Dequeue()
        {
            if (!_data.Any())
            {
                throw new InvalidOperationException("Queue is empty!");
            }

            var li = _data.Count - 1;
            var frontItem = _data[0];

            _data[0] = _data[li];
            _data.RemoveAt(li);

            --li;
            var pi = 0;
            while (true)
            {
                var ci = pi * 2 + 1;
                if (ci > li)
                {
                    break;
                }

                var rc = ci + 1;
                if (rc <= li && _data[rc].Key.CompareTo(_data[ci].Key) < 0)
                {
                    ci = rc;
                }

                if (_data[pi].Key.CompareTo(_data[ci].Key) <= 0)
                {
                    break;
                }

                Swap(pi, ci);
                pi = ci;
            }

            return frontItem;
        }

        private void Swap(int p, int c)
        {
            var tmp = _data[p];
            _data[p] = _data[c];
            _data[c] = tmp;
        }

        public override string ToString()
        {
            return $"[{_data.Count}]: {string.Join(" ", _data)}";
        }
    }
}
