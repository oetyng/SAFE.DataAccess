using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SAFE.DataAccess.LocalDb
{
    public class LocalMd : IMd
    {
        static Random _rand = new Random();

        readonly LocalMdData _data;
        readonly Dictionary<string, StoredValue> _stored = new Dictionary<string, StoredValue>();
        List<string> _keys = new List<string>();
        
        public int Count { get; private set; }
        public bool IsFull => Count > MdMetadata.Capacity;
        public int Level => _data.Level;
        public MdType Type => _data.Level > 0 ? MdType.Pointers : MdType.Values;
        public MdLocator MdLocator => _data.Locator;

        static readonly string _path;
        const string COLUMN_FAMILY = "MutableData";

        static LocalMd()
        {
            string temp = Path.GetTempPath();
            _path = Environment.ExpandEnvironmentVariables(Path.Combine(temp, "rocksdb_prefix_example"));
            var bbto = new BlockBasedTableOptions()
                .SetFilterPolicy(BloomFilterPolicy.Create(10, false))
                .SetWholeKeyFiltering(false);
            
        }

        private LocalMd(LocalMdData data)
        {
            _data = data;
            Count = data.Count;
        }

        //// level 0 gives new leaf 
        class LocalMdData
        {
            [NonSerialized]
            public MdLocator Locator;
            [NonSerialized]
            public string LocatorString;

            public int Level { get; set; }
            public int Count;
        }

        public static IMd Create(int level)
        {
            var locator = new MdLocator(new byte[32], DataProtocol.DEFAULT_PROTOCOL);
            _rand.NextBytes(locator.XORName);
            return Locate(locator, level);
        }

        static string GetString(MdLocator locator)
        {
            var xorString = $"{locator.XORName.Json()}_{locator.TypeTag}";
            return xorString;
        }

        static RocksDb GetRocksDb()
        {
            // try find on network
            var options = new DbOptions()
                .SetCreateIfMissing(true)
                .SetCreateMissingColumnFamilies(true);
            var columnFamilies = new ColumnFamilies
            {
                { COLUMN_FAMILY, new ColumnFamilyOptions() }
            };
            
            var db = RocksDb.Open(options, _path, columnFamilies);
            return db;
        }

        public static IMd Locate(MdLocator locator, int level = 0)
        {
            var xorString = GetString(locator);
            using (var db = GetRocksDb())
            {
                var metaKey = $"{xorString}_0"; // entry 0
                var column = db.GetColumnFamily(COLUMN_FAMILY);
                var value = db.Get(metaKey, cf: column);
                if (value == null) // if not found, create with level 0
                {
                    var data = new LocalMdData
                    {
                        Locator = locator,
                        LocatorString = xorString,
                        Level = level
                    };
                    value = data.Json();
                    db.Put(metaKey, new StoredValue(level).Json(), cf: column); // store level 0
                    return new LocalMd(data);
                }
                else
                {
                    var existing = new LocalMdData
                    {
                        Locator = locator,
                        LocatorString = xorString,
                        Level = value.Parse<StoredValue>().Payload.Parse<int>()
                    };

                    int.TryParse(db.Get($"{xorString}_Count", cf: column), out existing.Count);
                    
                    return new LocalMd(existing);
                }
            }
        }

        public Task<IEnumerable<(Pointer, StoredValue)>> GetAllPointerValuesAsync()
        {
            return Task.FromResult(GetAllPointerValues());
        }

        IEnumerable<(Pointer, StoredValue)> GetAllPointerValues()
        {
            using (var db = GetRocksDb())
            {
                var column = db.GetColumnFamily(COLUMN_FAMILY);
                var data = db.MultiGet(_keys.ToArray(), new[] { column });
                switch (Type)
                {
                    case MdType.Pointers:

                        return data
                            .Select(c => c.Value.Parse<Pointer>())
                            .Select(c => Locate(c.MdLocator))
                            .SelectMany(c => (c as LocalMd).GetAllPointerValues());
                    case MdType.Values:
                        return data
                            .ToDictionary(c => c.Key, c => c.Value.Parse<StoredValue>())
                            .Where(c => c.Value.ValueType != typeof(MdMetadata).Name)
                            .Select(c => (new Pointer
                            {
                                MdLocator = this.MdLocator,
                                MdKey = c.Key,
                                ValueType = c.Value.ValueType
                            }, c.Value));
                    default:
                        throw new ArgumentOutOfRangeException(nameof(Type));
                }
            }
        }

        IEnumerable<StoredValue> GetForPrefix(RocksDb db, ColumnFamilyHandle cf)
        {
            var readOptions = new ReadOptions();
            using (var iter = db.NewIterator(readOptions: readOptions, cf: cf))
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                var b = Encoding.UTF8.GetBytes($"{_data.LocatorString}");
                iter.Seek(b);
                while (iter.Valid())
                {
                    Console.WriteLine(iter.StringKey());
                    yield return iter.StringValue().Parse<StoredValue>();
                    iter.Next();
                }
            }
        }

        public Task<IEnumerable<StoredValue>> GetAllValuesAsync()
        {
            return Task.FromResult(GetAllValues());
        }

        IEnumerable<StoredValue> GetAllValues()
        {
            using (var db = GetRocksDb())
            {
                var column = db.GetColumnFamily(COLUMN_FAMILY);
                //var data = db.MultiGet(_keys.ToArray(), new[] { column });
                var data = new List<string>();
                foreach (var key in _keys)
                    data.Add(db.Get(key, column));
                switch (Type)
                {
                    case MdType.Pointers:
                        return data
                            .Select(c => c.Parse<Pointer>())
                            .Select(c => Locate(c.MdLocator))
                            .SelectMany(c => (c as LocalMd).GetAllValues());
                    case MdType.Values:
                        return data
                            .Select(c => c.Parse<StoredValue>())
                            .Where(c => c.ValueType != typeof(MdMetadata).Name);
                    default:
                        throw new ArgumentOutOfRangeException(nameof(Type));
                }
            }
        }

        public Task<Result<StoredValue>> GetValueAsync(string key)
        {
            return Task.FromResult(GetValue(key));
        }

        Result<StoredValue> GetValue(string key)
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return new InvalidOperation<StoredValue>($"There are no values in pointers. Method must be called on a ValuePointer (i.e. Md with Level = 0). Key {key}.");
                case MdType.Values:
                    using (var db = GetRocksDb())
                    {
                        var column = db.GetColumnFamily(COLUMN_FAMILY);
                        var value = db.Get(GetKey(key), cf: column);
                        if (value != null)
                            return Result.OK(value.Parse<StoredValue>());
                        else
                            return new KeyNotFound<StoredValue>($"Key: {key}");
                    }
                default:
                    return new ArgumentOutOfRange<StoredValue>(nameof(Type));
            }
        }

        public Task<Result<(Pointer, StoredValue)>> GetPointerAndValueAsync(string key)
        {
            return Task.FromResult(GetPointerAndValue(key));
        }

        Result<(Pointer, StoredValue)> GetPointerAndValue(string key)
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return new InvalidOperation<(Pointer, StoredValue)>($"There are no values in pointers. Method must be called on a ValuePointer (i.e. Md with Level = 0). Key {key}.");
                case MdType.Values:
                    using (var db = GetRocksDb())
                    {
                        var column = db.GetColumnFamily(COLUMN_FAMILY);
                        var value = db.Get(GetKey(key), cf: column);
                        if (value != null)
                        {
                            var stored = value.Parse<StoredValue>();
                            return Result.OK((new Pointer
                            {
                                MdLocator = this.MdLocator,
                                MdKey = key,
                                ValueType = stored.ValueType
                            }, stored));
                        }
                        else
                            return new KeyNotFound<(Pointer, StoredValue)>($"Key: {key}");
                    }
                default:
                    return new ArgumentOutOfRange<(Pointer, StoredValue)>(nameof(Type));
            }
        }

        public Task<Result<Pointer>> AddAsync(Pointer pointer)
        {
            return Task.FromResult(Add(pointer));
        }

        Result<Pointer> Add(Pointer pointer)
        {
            if (IsFull)
                return new MdOutOfEntriesError<Pointer>($"Filled: {Count}/{MdMetadata.Capacity}");
            var index = Count + 1;
            pointer.MdKey = index.ToString();
            using (var db = GetRocksDb())
            {
                var formattedKey = GetKey(pointer.MdKey);
                var column = db.GetColumnFamily(COLUMN_FAMILY);
                db.Put(formattedKey, new StoredValue(pointer).Json(), cf: column);
                db.Put($"{_data.LocatorString}_Count", $"{++Count}", cf: column);
                _keys.Add(formattedKey);
            }
            
            return Result.OK(pointer);
        }


        string GetKey(object key)
        {
            return $"{_data.LocatorString}_{key}";
        }

        public Task<Result<Pointer>> SetAsync(string key, StoredValue value, long expectedVersion = -1)
        {
            return Task.FromResult(Set(key, value));
        }

        Result<Pointer> Set(string key, StoredValue value)
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return new InvalidOperation<Pointer>($"Cannot set values directly on pointers. Key {key}, value type {value.ValueType}");
                case MdType.Values:
                    using (var db = GetRocksDb())
                    {
                        var formattedKey = GetKey(key);
                        var column = db.GetColumnFamily(COLUMN_FAMILY);
                        db.Put(formattedKey, value.Json(), cf: column);
                        _keys.Add(formattedKey);
                    }
                    return Result.OK(new Pointer
                    {
                        MdLocator = this.MdLocator,
                        MdKey = key,
                        ValueType = value.ValueType
                    });
                default:
                    return new ArgumentOutOfRange<Pointer>(nameof(Type));
            }
        }

        public Task<Result<Pointer>> DeleteAsync(string key)
        {
            return Task.FromResult(Delete(key));
        }

        Result<Pointer> Delete(string key)
        {
            switch (Type)
            {
                case MdType.Pointers:
                    throw new NotImplementedException("hmm...");
                case MdType.Values:
                    using (var db = GetRocksDb())
                    {
                        var formattedKey = GetKey(key);
                        var column = db.GetColumnFamily(COLUMN_FAMILY);
                        var value = db.Get(formattedKey, cf: column);
                        if (value == null)
                            return new KeyNotFound<Pointer>($"Key: {key}");
                        
                        db.Remove(formattedKey, cf: column);
                        return Result.OK(new Pointer
                        {
                            MdLocator = this.MdLocator,
                            MdKey = key,
                            ValueType = value.Parse<StoredValue>().ValueType
                        });
                    }
                default:
                    return new ArgumentOutOfRange<Pointer>(nameof(Type));
            }
        }

        public Task<Result<Pointer>> AddAsync(string key, StoredValue value)
        {
            return Task.FromResult(Add(key, value));
        }

        // It will return the direct pointer to the stored value
        // which makes it readily available for indexing at higher levels.
        Result<Pointer> Add(string key, StoredValue value)
        {
            if (IsFull)
                return new MdOutOfEntriesError<Pointer>($"Filled: {Count}/{MdMetadata.Capacity}");
            switch (Type)
            {
                case MdType.Pointers:
                    if (Count == 0)
                        return ExpandLevel(key, value);

                    IMd target;

                    using (var db = GetRocksDb())
                    {
                        var column = db.GetColumnFamily(COLUMN_FAMILY);
                        var stored = db.Get(GetKey(Count), cf: column);
                        if (value == null)
                            return new KeyNotFound<Pointer>($"Key: {key} in {_data.LocatorString}");

                        var pointer = value.Parse<StoredValue>().Payload.Parse<Pointer>();
                        target = Locate(pointer.MdLocator);
                    }

                    if (target.IsFull)
                        return ExpandLevel(key, value);
                    return (target as LocalMd).Add(key, value);

                case MdType.Values:
                    using (var db = GetRocksDb())
                    {
                        var formattedKey = GetKey(key);
                        var column = db.GetColumnFamily(COLUMN_FAMILY);
                        var stored = db.Get(formattedKey, cf: column);

                        //if (stored != null)
                        //    return new ValueAlreadyExists<Pointer>($"Key: {key}.");

                        if (stored == null)
                        {
                            db.Put(formattedKey, value.Json(), cf: column);
                            db.Put($"{_data.LocatorString}_Count", $"{++Count}", cf: column);
                            _keys.Add(formattedKey);
                        }
                        else if (!_keys.Contains(formattedKey))
                            _keys.Add(formattedKey);
                    }
                    return Result.OK(new Pointer
                    {
                        MdLocator = this.MdLocator,
                        MdKey = key,
                        ValueType = value.ValueType
                    });
                default:
                    return new ArgumentOutOfRange<Pointer>(nameof(Type));
            }
        }

        Result<Pointer> ExpandLevel(string key, StoredValue value)
        {
            if (Level == 0)
                return new ArgumentOutOfRange<Pointer>(nameof(Level));

            var md = LocalMd.Create(Level - 1);
            var leafPointer = (md as LocalMd).Add(key, value);
            if (!leafPointer.HasValue)
                return leafPointer;

            switch (md.Type)
            {
                case MdType.Pointers: // i.e. we have still not reached the end of the tree
                    Add(new Pointer
                    {
                        MdLocator = md.MdLocator,
                        ValueType = typeof(Pointer).Name
                    });
                    break;
                case MdType.Values:  // i.e. we are now right above leaf level
                    Add(leafPointer.Value);
                    break;
                default:
                    return new ArgumentOutOfRange<Pointer>(nameof(md.Type));
            }

            return leafPointer;
        }
    }
}
