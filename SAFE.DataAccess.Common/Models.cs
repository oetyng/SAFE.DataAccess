﻿
using Newtonsoft.Json;

namespace SAFE.DataAccess
{
    public enum MdType
    {
        Values = 0,
        Pointers = 1
    }

    public class MdMetadata
    {
        public const int Capacity = 999; // Since 1 entry is reserved for metadata itself.
    }

    public static class DataProtocol
    {
        public const ulong DEFAULT_PROTOCOL = 20100;
        public const ulong MD_HEAD = 20101;
        public const ulong MD_POINTER = 20102;
        public const ulong MD_VALUE = 20103;
    }

    public class MdLocator
    {
        [JsonConstructor]
        MdLocator() { }

        public MdLocator(byte[] xorName, ulong typeTag)
        {
            XORName = xorName;
            TypeTag = typeTag;
        }
        public byte[] XORName { get; set; } // The address of the Md this points at.
        public ulong TypeTag { get; set; } // Md tag type
    }

    public class Pointer
    {
        public MdLocator MdLocator { get; set; } // The address of the Md this points at.
        public string MdKey { get; set; } // The key under which the value is stored in that Md.
        public string ValueType { get; set; } // The type of the value stored.
    }

    public class StoredValue
    {
        [JsonConstructor]
        StoredValue(){}

        public StoredValue(object data)
        {
            Payload = data.Json();
            ValueType = data.GetType().Name;
        }

        public string Payload { get; set; }
        public string ValueType { get; set; }

        public T Parse<T>()
        {
            return Payload.Parse<T>();
        }
    }
}
