using System;
using System.Collections.Generic;

namespace SAFE.DataAccess
{
    public class MdMetadata
    {
        Dictionary<string, string> _data = new Dictionary<string, string>();

        public const int Capacity = 1000;
        const string TYPE_KEY = "TYPE";
        const string VERSION_KEY = "VERSION";
        public const string LEVEL_KEY = "LEVEL";
        public const string COUNT_KEY = "COUNT";
        public const string XOR_ADDRESS_KEY = "XOR_ADDRESS";

        public MdType Type;
        public int Level;
        public int Count;
        public byte[] XORAddress;
        public ulong MetadataVersion;
        

        public MdMetadata(int level)
        {
            Level = level;
            Set(LEVEL_KEY, level);
            Type = level == 0 ? MdType.Values : MdType.Pointers;
            Set(TYPE_KEY, Type);
        }

        public bool ContainsKey(string key)
        {
            return _data.ContainsKey(key);
        }

        public T Get<T>(string key)
        {
            return _data[key].Parse<T>();
        }

        public object Get(string key)
        {
            return _data[key].Parse();
        }

        public void Set(string key, object value)
        {
            if (key == XOR_ADDRESS_KEY)
                XORAddress = (byte[])value;
            _data[key] = value.Json();
        }

        public void IncrementCount()
        {
            ++Count;
            Set(COUNT_KEY, Count);
        }

        public void DecrementCount()
        {
            --Count;
            Set(COUNT_KEY, Count);
        }

        public void IncrementVersion()
        {
            ++Count;
            Set(VERSION_KEY, MetadataVersion);
        }

        public void DecrementVersion()
        {
            --Count;
            Set(VERSION_KEY, MetadataVersion);
        }

        public virtual T Clone<T>() where T : MdMetadata
        {
            var clone = Activator.CreateInstance<T>();
            foreach (var pair in _data)
                clone._data[pair.Key] = pair.Value;
            return clone;
        }
    }
}
