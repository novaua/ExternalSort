using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Common;

namespace ExternalSort.Tests
{
    [TestClass]
    public class GenericTests
    {
        [TestMethod]
        public void TestStringBytes()
        {
            var testString = "100501. Hello string!";
            var testStringBytes = Encoding.UTF8.GetBytes(testString);

            Console.Write($"Count {testString.Length}, bytes {testStringBytes.Length}");
        }

        [TestMethod]
        public void BytesFormat()
        {
            var varints = new[]
            {
                BytesFormatter.Format(0),
                BytesFormatter.Format(1),
                BytesFormatter.Format(100),
                BytesFormatter.Format(1024),
                BytesFormatter.Format(1025),
                BytesFormatter.Format(7 * 1024 * 1024),
                BytesFormatter.Format(58L * 1024 * 1024 * 1024),
                BytesFormatter.Format(100500L * 1024 * 1024 * 1024),
                BytesFormatter.Format(1022342342345),
                BytesFormatter.Format(long.MaxValue),
            };

            Assert.AreEqual("0 bytes", varints[0]);
            Assert.AreEqual("7 MB", varints[5]);

            foreach (var v in varints)
            {
                Console.WriteLine(v);
            }
        }

        [TestMethod]
        public void TestLinesCopmparer()
        {
            var lines = new List<string>
            {
                "23. Apple",
                "100500. Hello",
                "44. Hello",
                "0. Hello",
                "3. Apple",
                "23. End!",
            };

            lines.Sort(new LineComparer());

            Assert.AreEqual("3. Apple", lines.First());
            Assert.AreEqual("100500. Hello", lines.Last());
        }

        [TestMethod]
        public void TestBadLinesCopmparer()
        {
            var lines = new List<string>
            {
                "23. Apple",
                "100500. Hello",
                "44. Hello",
                "End!",
                "0. Hello",
                "3. Apple",
            };

            lines.Sort(new LineComparer());

            Assert.AreEqual("3. Apple", lines.First());
            Assert.AreEqual("End!", lines.Last());
        }
    }
}
