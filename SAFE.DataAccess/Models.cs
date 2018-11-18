using System.Collections.Generic;

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

    public class Metadata
    {
        Dictionary<string, string> _data = new Dictionary<string, string>();

        public const string LEVEL_KEY = "LEVEL";
        public const string COUNT_KEY = "COUNT";
        public const string XOR_ADDRESS_KEY = "XOR_ADDRESS";

        public int Level;
        public int Count;
        public const int Capacity = 1000;
        public byte[] XORAddress;

        public Metadata(int level)
        {
            Level = level;
            Set(LEVEL_KEY, level);
        }

        public void Set(string key, object value)
        {
            if (key == XOR_ADDRESS_KEY)
                XORAddress = (byte[])value;
            _data[key] = value.Json();
        }

        public T Get<T>(string key)
        {
            return _data[key].Parse<T>();
        }

        public void IncrementCount()
        {
            ++Count;
            Set(COUNT_KEY, Count);
        }
    }
}
