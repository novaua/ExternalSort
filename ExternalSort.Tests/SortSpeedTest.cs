using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using Common;

namespace ExternalSort.Tests
{
    [TestClass]
    public class SortSpeedTest
    {
        [TestMethod]
        public void TestMethod1()
        {
            var inputDir = @"..\..\..\bin\Debug";
            var files = Directory.EnumerateFiles(inputDir);
            foreach (var file in files.Where(x => Path.GetFileName(x).StartsWith("input") && x.EndsWith(".txt")))
            {
                SortFile(file);
            }
        }

        private void SortFile(string inputFile)
        {
            var fi = new FileInfo(inputFile);
            if (!fi.Exists)
            {
                throw new ArgumentException($"File '{inputFile}' does not exist!");
            }

            using (var asw = new AutoStopwatch($"Sorting file '{inputFile}' {BytesFormatter.Format(fi.Length)} ", fi.Length))
            {
                var lines = File.ReadAllLines(inputFile);
                Array.Sort(lines);
            }
        }
    }
}
