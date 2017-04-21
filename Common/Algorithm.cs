using System;
using System.Text;

namespace Common
{
    public static class Algorithm
    {
        public static readonly byte[] EolUtf8Bytes = Encoding.UTF8.GetBytes(Environment.NewLine);

        /// <summary>
        /// Finds subsequence of bytes inside another byte array
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="searchedBytes"></param>
        /// <returns>Offset of the subsequence or -1 of the subsequence was not found</returns>
        public static int FindByteSubsequence(byte[] buffer, byte[] searchedBytes)
        {
            for (var i = 0; i < buffer.Length - searchedBytes.Length; ++i)
            {
                var match = false;
                for (var j = 0; j < searchedBytes.Length; ++j)
                {
                    if (buffer[i + j] == searchedBytes[j])
                    {
                        match = true;
                    }
                    else
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                {
                    return i;
                }
            }

            return -1;
        }

        public static int FindEolOffset(byte[] buffer)
        {
            return FindByteSubsequence(buffer, EolUtf8Bytes);
        }
    }
}
