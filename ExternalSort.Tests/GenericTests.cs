using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
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
                BytesFormatter.Format(7*1024*1024),
                BytesFormatter.Format(58L*1024*1024*1024),
                BytesFormatter.Format(100500L*1024*1024*1024),
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

        [TestMethod]
        public void LineSplit_Test()
        {
            var line1 = "One ";
            var line2 = "Two";
            var lines = line1 + Environment.NewLine + line2;

            var lb = Encoding.UTF8.GetBytes(lines);
            var sr = Algorithm.EndLineSplit(lb);
            Assert.AreEqual(line2, Encoding.UTF8.GetString(sr.Item2));
            Assert.AreEqual(line1 + Environment.NewLine, Encoding.UTF8.GetString(sr.Item1));
        }

        [TestMethod]
        public void FileSplit_Test()
        {
            var maxLines = 2345;

            var tempFile = Path.GetTempFileName();
            File.WriteAllLines(tempFile, GenerateLines(maxLines));

            var fileLen = new FileInfo(tempFile).Length;
            var perFile = fileLen/4;

            var tempRoot = Path.GetDirectoryName(tempFile);
            var mask = Path.Combine(tempRoot, "vitalys_temp_file_n{0}");

            var outList = new List<string>();

            Algorithm.SplitTextFile(tempFile, mask, perFile, x => outList.Add(x));

            var totalLen = 0L;
            foreach (var file in outList)
            {
                var len = new FileInfo(file).Length;
                totalLen += len;

                Console.WriteLine($"{file}\t{len}");
            }

            Assert.AreEqual(totalLen, fileLen);

            outList.Add(tempFile);
            foreach (var file in outList)
            {
                File.Delete(file);
            }
        }

        IEnumerable<string> GenerateLines(int count = 500)
        {
            var rand = new Random();
            for (int i = 0; i < count; i++)
            {
                var nextInt = rand.Next();
                yield return $"{nextInt}. {Guid.NewGuid()}";
            }
        }
    }
}