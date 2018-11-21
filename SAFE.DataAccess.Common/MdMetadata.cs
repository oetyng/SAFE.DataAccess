using Newtonsoft.Json;

namespace SAFE.DataAccess
{
    public class MdMetadata
    {
        public const int Capacity = 999; // Since 1 entry is reserved for metadata itself.

        const string TYPE_KEY = "TYPE";
        const string VERSION_KEY = "VERSION";
        public const string LEVEL_KEY = "LEVEL";
        public const string COUNT_KEY = "COUNT";
        public const string MD_LOCATION_KEY = "MD_LOCATION";

        public MdType Type { get; private set; }
        public int Level { get; private set; }
        public MdLocation MdLocation { get; private set; }

        public int Count { get; private set; }
        public ulong MetadataVersion { get; private set; }

        [JsonConstructor]
        MdMetadata()
        { }

        public MdMetadata(int level, MdLocation location)
        {
            Level = level;
            Type = level == 0 ? MdType.Values : MdType.Pointers;
            MdLocation = location;
        }

        public void IncrementCount()
        {
            Count++;
        }

        public void DecrementCount()
        {
            Count--;
        }

        public void IncrementVersion()
        {
            MetadataVersion++;
        }

        public void DecrementVersion()
        {
            MetadataVersion--;
        }

        public MdMetadata Clone()
        {
            var clone = new MdMetadata(Level, MdLocation)
            {
                Count = this.Count,
                MetadataVersion = this.MetadataVersion
            };
            return clone;
        }
    }
}
