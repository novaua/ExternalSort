using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

namespace ExternalSort
{
    public class PriorityQueue<T> where T : IComparable<T>
    {
        private readonly IComparer<T> _comparer;
        private readonly List<T> _data;

        public PriorityQueue()
        {
            _comparer = Comparer<T>.Default;
            _data = new List<T>();
        }

        public PriorityQueue(IComparer<T> comparer)
        {
            _comparer = comparer;
            _data = new List<T>();
        }

        public int Count => _data.Count;

        public T Peek()
        {
            var frontItem = _data[0];
            return frontItem;
        }

        public void Enqueue(T item)
        {
            _data.Add(item);
            var ci = _data.Count - 1;
            while (ci > 0)
            {
                var pi = (ci - 1) / 2;
                if (_comparer.Compare(_data[ci], _data[pi]) >= 0)
                {
                    break;
                }

                Swap(ci, pi);
                ci = pi;
            }
        }

        public T Dequeue()
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
                if (rc <= li && _data[rc].CompareTo(_data[ci]) < 0)
                {
                    ci = rc;
                }

                if (_data[pi].CompareTo(_data[ci]) <= 0)
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
