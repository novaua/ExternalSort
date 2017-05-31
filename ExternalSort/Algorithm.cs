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

            var inputFileSize = new FileInfo(baseFile).Length;
            var filesCount = (inputFileSize / maxSize) + (inputFileSize % maxSize == 0 ? 0 : 1);

            var blocksCount = maxSize / buffer.Length + (maxSize % buffer.Length == 0 ? 0 : 1);

            using (var file = File.OpenRead(baseFile))
            {
                var allRead = false;
                for (var i = 0; i < filesCount; i++)
                {
                    if (allRead)
                    {
                        break;
                    }

                    var outFileName = string.Format(outFileMask, i);
                    using (var destFile = File.Open(outFileName, FileMode.Create, FileAccess.ReadWrite))
                    {
                        for (var j = 0; j < blocksCount && !allRead; j++)
                        {
                            var rc = file.Read(buffer, 0, buffer.Length);
                            if (rc > 0)
                            {
                                destFile.Write(buffer, 0, rc);
                            }
                            else
                            {
                                allRead = true;
                            }
                        }

                        if (!allRead)
                        {
                            var splitBuffer = destFile.Length < buffer.Length
                                ? new byte[destFile.Length]
                                : buffer;

                            destFile.Seek(splitBuffer.Length, SeekOrigin.End);
                            destFile.Read(splitBuffer, 0, splitBuffer.Length);
                            destFile.SetLength(destFile.Length - splitBuffer.Length);

                            var lastLinex = LastEndLineSplit(splitBuffer);
                            destFile.Write(lastLinex.Item1, 0, lastLinex.Item1.Length);

                            Console.WriteLine("End of line found at from end offset {0}", lastLinex.Item2.Length);

                            file.Seek(-1 * lastLinex.Item2.Length, SeekOrigin.Current);
                            allRead = false;
                        }
                    }

                    outFileReady(outFileName);
                }
            }
        }

        /// <summary>
        /// This is not super efficient but simple coding and good for moderate size of input.
        /// </summary>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public static Tuple<byte[], byte[]> LastEndLineSplit(byte[] buffer)
        {
            var endl = Environment.NewLine;

            var stringBuffer = Encoding.UTF8.GetString(buffer);
            var endlPos = stringBuffer.LastIndexOf(endl, StringComparison.InvariantCulture);
            if (endlPos == -1)
            {
                throw new ArgumentException("End line was not found. The input is not a text!");
            }

            var head = stringBuffer.Substring(0, endlPos + endl.Length);
            var tail = stringBuffer.Substring(endlPos + endl.Length);
            return new Tuple<byte[], byte[]>(Encoding.UTF8.GetBytes(head), Encoding.UTF8.GetBytes(tail));
        }
    }
}
