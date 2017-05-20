using System;
using System.IO;
using System.Text;

namespace ExternalSort
{
    public class Algorithm
    {
        public static void SplitTextFile(string baseFile, string outFileMask, long outFileSize, Action<string> outFileReady)
        {
            var maxSize = outFileSize;
            var buffer = new byte[1024];
            var blocksCount = maxSize / buffer.Length;
            var inputFileSize = new FileInfo(baseFile).Length;
            var filesCount = (inputFileSize / maxSize) + (inputFileSize % maxSize == 0 ? 0 : 1);

            using (var file = File.OpenRead(baseFile))
            {
                for (var i = 0; i < filesCount; i++)
                {
                    var outFileName = string.Format(outFileMask, i);
                    using (var destFile = File.Open(outFileName, FileMode.Create, FileAccess.ReadWrite))
                    {
                        for (var j = 0; j < blocksCount; j++)
                        {
                            var rc = file.Read(buffer, 0, buffer.Length);
                            destFile.Write(buffer, 0, rc);
                        }

                        var splitBuffer = destFile.Length < buffer.Length
                            ? new byte[destFile.Length]
                            : buffer;

                        destFile.Seek(splitBuffer.Length, SeekOrigin.End);
                        destFile.Read(splitBuffer, 0, splitBuffer.Length);
                        destFile.SetLength(destFile.Length - splitBuffer.Length);

                        var lastLinex = EndLineSplit(splitBuffer);
                        destFile.Write(lastLinex.Item1, 0, lastLinex.Item1.Length);

                        Console.WriteLine("End of line found at from end offset {0}", lastLinex.Item2.Length);

                        file.Seek(-1 * lastLinex.Item2.Length, SeekOrigin.Current);
                    }

                    outFileReady(outFileName);
                }
            }
        }

        public static Tuple<byte[], byte[]> EndLineSplit(byte[] buffer)
        {
            var endl = Environment.NewLine;
            var endlBytes = Encoding.UTF8.GetBytes(endl);
            var found = -1;
            for (var i = 0; i != buffer.Length; i++)
            {
                var matchCount = 0;
                for (var j = 0; j != endlBytes.Length && i + endlBytes.Length < buffer.Length; ++j)
                {
                    if (buffer[i + j] == endlBytes[j])
                    {
                        ++matchCount;
                    }
                    else
                    {
                        break;
                    }
                }

                if (matchCount == endlBytes.Length)
                {
                    found = i;
                    break;
                }
            }

            if (found >= 0)
            {
                if (found + endlBytes.Length == buffer.Length)
                {
                    return new Tuple<byte[], byte[]>(buffer, null);
                }

                var head = new byte[found + endlBytes.Length];
                Array.Copy(buffer, 0, head, 0, head.Length);
                var tail = new byte[buffer.Length - head.Length];

                Array.Copy(buffer, found + endlBytes.Length, tail, 0, tail.Length);

                return new Tuple<byte[], byte[]>(head, tail);
            }

            throw new ArgumentException("Unable to find end of line");
        }
    }
}
