using System;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Common;

namespace ExternalSort.Tests
{
    [TestClass]
    public class Generic
    {
        [TestMethod]
        public void TestMethod1()
        {
            IdString o1;
            var in1 = "23.Hello world!";
            IdString.TryMakeIdString(in1, out o1);
            Assert.AreEqual(o1.ToString(), in1);

            IdString o2;
            var in2 = "23. Hello .world!";
            IdString.TryMakeIdString(in2, out o2);
            Assert.AreEqual(o2.ToString(), in2);

            Assert.AreEqual(o1.Id, o2.Id);

            in1 = "23 Hello world!";
            IdString o3;
            IdString.TryMakeIdString(in1, out o3);
            Assert.AreEqual(default(IdString), o3);
        }

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

            foreach (var v in varints)
            {
                Console.WriteLine(v);
            }
        }
    }
}
