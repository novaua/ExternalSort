using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ExternalSort.Tests
{
    [TestClass]
    public class CountedListTests
    {
        const int DefaultCount = 12;
        private CountedList _countedList;

        [TestInitialize]
        public void TestInitialize()
        {
            _countedList = new CountedList(DefaultCount * 4);
        }

        [TestCleanup]
        public void CleanUp()
        {
            // nothing
        }

        [TestMethod]
        public void BasicTest()
        {
            var cc = 6 * DefaultCount + 3;
            var exepectedCount = 7;

            var count = 0;
            _countedList.MaxIntemReached = (l, ic) =>
            {
                Assert.IsTrue(ic <= DefaultCount * 4);
                ++count;
            };

            for (int i = 0; i < cc; i++)
            {
                _countedList.Add("Item");
            }

            _countedList.Dispose();
            Assert.AreEqual(exepectedCount, count);
        }
    }
}
