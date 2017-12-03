using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ExternalSort.Tests
{
    [TestClass]
    public class NWayMergerTests
    {
        private NWayMerger _merger;

        [TestInitialize]
        public void TestInitialize()
        {
            _merger = new NWayMerger();
        }

        [TestCleanup]
        public void CleanUp()
        {
            _merger = null;
        }

        [TestMethod]
        public void SingleTread_Test()
        {
            var maxLines = 100;
            var maxFiles = 10;

            var inputFiles = GenerateInputFiles(maxFiles, () => Guid.NewGuid().ToString(), maxLines).ToList();
            var totalLength = inputFiles.Sum(x => new FileInfo(x).Length);

            var outputFile = Path.GetTempFileName();
            var totalLines = 0;

            _merger.MergeFiles(inputFiles, outputFile, File.OpenText, File.OpenWrite, x => totalLines += (int)x);

            Assert.AreEqual(maxFiles * maxLines, totalLines);
            Assert.AreEqual(totalLength, new FileInfo(outputFile).Length);
            Assert.IsTrue(IsSortedFile(outputFile));
        }

        private IEnumerable<string> GenerateInputFiles(int fileCount, Func<string> nextLineFactory, int linesPerFile = 1024)
        {
            var allLines = new string[linesPerFile];

            for (int i = 0; i < fileCount; i++)
            {
                var tempFile = Path.GetTempFileName();    
                for (var j = 0; j < linesPerFile; ++j)
                {
                    var line = nextLineFactory();
                    allLines[j] = line;
                }

                Array.Sort(allLines);
                File.WriteAllLines(tempFile, allLines);

                yield return tempFile;
            }
        }

        private bool IsSortedFile(string fileName)
        {
            var lines = File.ReadAllLines(fileName);
            if (lines.Length == 1)
            {
                return true;
            }

            var prevLine = lines.First();
            var invarinant = false;
            foreach (var line in lines.Skip(1))
            {
                invarinant = prevLine.CompareTo(line) <= 0;
                if (!invarinant)
                    break;
            }

            return invarinant;
        }
    }
}
