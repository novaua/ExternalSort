using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ExternalSort.Tests
{
    [TestClass]
    public class PriorityQueueTests
    {
        [TestMethod]
        public void BasicTest()
        {
            int maxCount = 10000;
            var r = new Random();
            var pq = new PriorityQueue<int>();
            for (int i = 0; i < maxCount; i++)
            {
                pq.Enqueue(r.Next());
            }

            Assert.AreEqual(maxCount, pq.Count);
            var min = int.MinValue;
            for (int i = 0; i < maxCount; i++)
            {
                var curr = pq.Dequeue();
                Assert.IsTrue(curr >= min);
                min = curr;
            }
        }
    }
}
