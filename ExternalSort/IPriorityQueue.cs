using System;

namespace ExternalSort
{
    public interface IPriorityQueue<T> where T : IComparable<T>
    {
        int Count { get; }

        T Peek();

        void Enqueue(T item);

        T Dequeue();
    }
}
