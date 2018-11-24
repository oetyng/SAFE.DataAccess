using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    // This is the only in-memory mock in the project.
    // To connect to SAFENetwork (mock/local/alpha-2), implement the IMd interface
    // with connections via Maidsafe's SafeApp library.
    public class Md : IMd
    {
        static Dictionary<string, IMd> _allMds = new Dictionary<string, IMd>();

        Dictionary<string, StoredValue> _valueFields = new Dictionary<string, StoredValue>();
        Dictionary<int, Pointer> _pointerFields = new Dictionary<int, Pointer>();

        public int Level { get; set; }
        public int Count { get; set; }
        public MdLocator MdLocator { get; set; }
        public bool IsFull => Count > MdMetadata.Capacity;
        public MdType Type { get; set; }

        private Md(int level)
        {
            MdLocator = new MdLocator(new byte[32], DataProtocol.DEFAULT_PROTOCOL);
            _rand.NextBytes(MdLocator.XORName);
            _valueFields["0"] = new StoredValue(level);
        }

        Random _rand = new Random();
        // level 0 gives new leaf 
        public static IMd Create(int level)
        {
            var newMd = new Md(level);
            _allMds[newMd.MdLocator.Json()] = newMd;
            return newMd;
        }

        public static IMd Locate(MdLocator location)
        {
            // try find on network
            var key = location.Json();
            if (!_allMds.ContainsKey(key)) // if not found, create with level 0
            {
                var md = Md.Create(level: 0);
                _allMds[md.MdLocator.Json()] = md;
                return md;
            }

            return _allMds[key];
        }

        public Task<IEnumerable<(Pointer, StoredValue)>> GetAllPointerValuesAsync()
        {
            return Task.FromResult(GetAllPointerValues());
        }

        IEnumerable<(Pointer, StoredValue)> GetAllPointerValues()
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return _pointerFields.Values
                        .Select(c => Locate(c.MdLocator))
                        .SelectMany(c => (c as Md).GetAllPointerValues());
                case MdType.Values:
                    return _valueFields
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

        public Task<IEnumerable<StoredValue>> GetAllValuesAsync()
        {
            return Task.FromResult(GetAllValues());
        }

        IEnumerable<StoredValue> GetAllValues()
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return _pointerFields.Values
                        .Select(c => Locate(c.MdLocator))
                        .SelectMany(c => (c as Md).GetAllValues());
                case MdType.Values:
                    return _valueFields
                        .Select(c => c.Value)
                        .Where(c => c.ValueType != typeof(MdMetadata).Name);
                default:
                    throw new ArgumentOutOfRangeException(nameof(Type));
            }
        }

        public Task<Result<StoredValue>> GetValueAsync(string key)
        {
            return Task.FromResult(GetValue(key));
        }

        Result<StoredValue> GetValue(string key)
        {
            switch(Type)
            {
                case MdType.Pointers:
                    return new InvalidOperation<StoredValue>($"There are no values in pointers. Method must be called on a ValuePointer (i.e. Md with Level = 0). Key {key}.");
                case MdType.Values:
                    if (_valueFields.ContainsKey(key))
                        return Result.OK(_valueFields[key]);
                    else
                        return new KeyNotFound<StoredValue>($"Key: {key}");
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
                    if (_valueFields.ContainsKey(key))
                    {
                        var value = _valueFields[key];
                        return Result.OK((new Pointer
                        {
                            MdLocator = this.MdLocator,
                            MdKey = key,
                            ValueType = value.ValueType
                        }, value));
                    }
                    else
                        return new KeyNotFound<(Pointer, StoredValue)>($"Key: {key}");
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
            _pointerFields[index] = pointer;
            Count++;
            return Result.OK(pointer);
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
                    _valueFields[key] = value;
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
                    if (!_valueFields.ContainsKey(key))
                        return new KeyNotFound<Pointer>($"Key: {key}");
                    var value = _valueFields[key];
                    _valueFields.Remove(key);
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

                    var target = Locate(_pointerFields[Count].MdLocator);
                    if (target.IsFull) 
                        return ExpandLevel(key, value);

                    return (target as Md).Add(key, value);
                case MdType.Values:
                    if (_valueFields.ContainsKey(key))
                        return new ValueAlreadyExists<Pointer>($"Key: {key}.");

                    _valueFields[key] = value;
                    Count++;
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

            var md = Md.Create(Level - 1);
            var leafPointer = (md as Md).Add(key, value);
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
