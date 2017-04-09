using System;
using System.Linq;

namespace ExternalSort
{
    public struct IdString : IComparable<IdString>
    {
        public long Id { get; set; }

        public string Alpha { get; set; }

        public int CompareTo(IdString other)
        {
            var result = string.Compare(Alpha, other.Alpha, StringComparison.InvariantCulture);
            if (result == 0)
            {
                result = Id.CompareTo(other.Id);
            }

            return result;
        }

        public static bool TryMakeIdString(string line, out IdString result)
        {
            var idAlpaStr = line.Split('.');
            result = new IdString();
            if (idAlpaStr.Any())
            {
                long id;
                if (long.TryParse(idAlpaStr[0], out id))
                {
                    result.Id = id;
                    result.Alpha = (idAlpaStr.Length > 1) ? string.Join(".", idAlpaStr.Skip(1)) : string.Empty;
                    return true;
                }
            }

            return false;
        }

        public override string ToString()
        {
            return $"{Id}.{Alpha}";
        }
    }
}
