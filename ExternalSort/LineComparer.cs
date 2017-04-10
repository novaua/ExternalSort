using System;
using System.Collections.Generic;
using System.Globalization;

namespace ExternalSort
{
    public class LineComparer : IComparer<string>
    {
        /// <summary>
        /// This method tries hart to follow the spec. rules but nevertheless it never fails.
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        public int Compare(string x, string y)
        {
            if (string.IsNullOrEmpty(x) || string.IsNullOrEmpty(y))
            {
                return string.Compare(x, y, CultureInfo.CurrentCulture, CompareOptions.StringSort);
            }

            var xp = OneSplit(x);
            var yp = OneSplit(y);

            var result = string.Compare(xp.Item1, yp.Item1, CultureInfo.CurrentCulture, CompareOptions.StringSort);
            if (result == 0)
            {
                long xId;
                var resultSet = false;
                if (long.TryParse(xp.Item2, out xId))
                {
                    long yId;
                    if (long.TryParse(yp.Item2, out yId))
                    {
                        result = xId.CompareTo(yId);
                        resultSet = true;
                    }
                }

                if (!resultSet)
                {
                    result = string.Compare(xp.Item2, yp.Item2, CultureInfo.CurrentCulture, CompareOptions.StringSort);
                }
            }

            return result;
        }

        private Tuple<string, string> OneSplit(string line, string delimeiter = ".")
        {
            var pos = line.IndexOf('.');
            if (pos >= 0)
            {
                return new Tuple<string, string>(line.Substring(pos + 1), line.Substring(0, pos));
            }

            return new Tuple<string, string>(line, string.Empty);
        }
    }
}
