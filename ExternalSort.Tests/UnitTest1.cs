using System;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ExternalSort.Tests
{
    [TestClass]
    public class UnitTest1
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

            in1 = "23 Hello world!";
            IdString.TryMakeIdString(in1, out o1);
            Assert.AreEqual(default(IdString), in1);

            Assert.AreEqual(o1.Id, o2.Id);
        }

        [TestMethod]
        public void TestStringBytes()
        {
            var testString = "100501. Hello string!";
            var testStringBytes = Encoding.UTF8.GetBytes(testString);

            Console.Write($"Count {testString.Length}, bytes {testStringBytes.Length}");
        }
    }
}
