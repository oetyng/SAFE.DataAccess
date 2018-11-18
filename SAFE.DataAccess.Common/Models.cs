
namespace SAFE.DataAccess
{
    public enum MdType
    {
        Values,
        Pointers
    }

    public class Pointer
    {
        public byte[] XORAddress { get; set; } // The address of the Md this points at.
        public string MdKey { get; set; } // The key under which the value is stored in that Md.
        public string ValueType { get; set; } // The type of the value stored.
    }

    public class Value
    {
        public Value()
        { }

        public Value(object data)
        {
            Payload = data.Json();
            ValueType = data.GetType().Name;
        }

        public string Payload { get; set; }
        public string ValueType { get; set; }
    }
}
