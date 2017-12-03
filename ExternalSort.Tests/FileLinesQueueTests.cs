using System;
using System.IO;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ExternalSort.Tests
{
    [TestClass]
    public class FileLinesQueueTests
    {
        private const int MaxTestLines = 128;
        private AutoFileQueue _queue;
        private string _tempFile;

        [TestInitialize]
        public void TestInitialize()
        {
            _tempFile = Path.GetTempFileName();
        }

        [TestCleanup]
        public void CleanUp()
        {
            if (_queue != null)
            {
                _queue.Dispose();
                File.Delete(_tempFile);
            }
        }

        [TestMethod]
        public void TestBasics()
        {
            MakeQueue();
            for (var i = 0; i < MaxTestLines; ++i)
            {
                Guid guid;
                Assert.IsTrue(Guid.TryParse(_queue.Dequeue(), out guid));
            }

            Assert.IsFalse(_queue.Any());
        }

        [TestMethod]
        public void TestEmptyFile()
        {
            using (var myQueue = new AutoFileQueue(new StreamReader(_tempFile), CancellationToken.None))
            {
                Assert.IsFalse(myQueue.Any());
            }
        }

        private void MakeQueue()
        {
            using (var sw = new StreamWriter(_tempFile))
            {
                for (int i = 0; i < MaxTestLines; i++)
                {
                    sw.WriteLine(Guid.NewGuid().ToString());
                }
            }

            _queue = new AutoFileQueue(new StreamReader(_tempFile), CancellationToken.None, MaxTestLines / 8);
        }
    }
}
