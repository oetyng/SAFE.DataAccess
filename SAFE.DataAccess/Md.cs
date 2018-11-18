using System;
using System.Collections.Generic;
using System.Linq;

namespace SAFE.DataAccess
{
    public class Md : IMd
    {
        static Dictionary<string, Md> _allMds = new Dictionary<string, Md>();

        const string TYPE_KEY = "TYPE";
        const string LEVEL_KEY = "LEVEL";
        Dictionary<string, Value> _valueFields = new Dictionary<string, Value>();
        Dictionary<int, Pointer> _pointerFields = new Dictionary<int, Pointer>();
        Metadata _metadata;

        Metadata Metadata
        {
            get
            {
                if (_metadata == null)
                    _metadata = _valueFields["metadata"].Payload.Parse<Metadata>();
                return _metadata;
            }
        }

        public byte[] XORAddress => Metadata.XORAddress;
        public int Level => Metadata.Level;
        public int Count => Metadata.Count;
        public bool IsFull => Metadata.Count > Metadata.Capacity;
        public MdType Type => Metadata.Get<MdType>(TYPE_KEY);

        private Md(int level)
        {
            _valueFields["metadata"] = new Value
            {
                Payload = new Metadata(level).Json(),
                ValueType = typeof(Metadata).Name
            };
            Metadata.Set(TYPE_KEY, level == 0 ? MdType.Values : MdType.Pointers);
            
            var xor = new byte[32];
            _rand.NextBytes(xor);
            Metadata.Set(Metadata.XOR_ADDRESS_KEY, xor);
        }

        Random _rand = new Random();
        // level 0 gives new leaf 
        public static Md Create(int level)
        {
            var newMd = new Md(level);
            _allMds[newMd.XORAddress.Json()] = newMd;
            //newMd.Metadata.Set(LEVEL_KEY, level);
            //newMd.Metadata.Set(TYPE_KEY, level == 0 ? MdType.Values : MdType.Pointers);
            return newMd;
        }

        public static Md Locate(byte[] xorAddress)
        {
            // try find on network
            var key = xorAddress.Json();
            if (!_allMds.ContainsKey(key)) // if not found, create with level 0
            {
                var md = Md.Create(level: 0);
                _allMds[md.XORAddress.Json()] = md;
                return md;
            }

            return _allMds[key];
        }

        public IEnumerable<(Pointer, Value)> GetAllPointerValues()
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return _pointerFields.Values
                        .Select(c => Locate(c.XORAddress))
                        .SelectMany(c => c.GetAllPointerValues());
                case MdType.Values:
                    return _valueFields
                        .Where(c => c.Value.ValueType != typeof(Metadata).Name)
                        .Select(c => (new Pointer
                        {
                            XORAddress = this.XORAddress,
                            MdKey = c.Key,
                            ValueType = c.Value.ValueType
                        }, c.Value));
                default:
                    throw new ArgumentOutOfRangeException(nameof(Type));
            }
        }

        public IEnumerable<Value> GetValues()
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return _pointerFields.Values
                        .Select(c => Locate(c.XORAddress))
                        .SelectMany(c => c.GetValues());
                case MdType.Values:
                    return _valueFields
                        .Select(c => c.Value)
                        .Where(c => c.ValueType != typeof(Metadata).Name);
                default:
                    throw new ArgumentOutOfRangeException(nameof(Type));
            }
        }

        public Result<Value> GetValue(string key)
        {
            switch(Type)
            {
                case MdType.Pointers:
                    return new InvalidOperation<Value>($"There are no values in pointers. Method must be called on a ValuePointer (i.e. Md with Level = 0). Key {key}.");
                case MdType.Values:
                    if (_valueFields.ContainsKey(key))
                        return Result.OK(_valueFields[key]);
                    else
                        return new KeyNotFound<Value>($"Key: {key}");
                    
                default:
                    return new ArgumentOutOfRange<Value>(nameof(Type));
            }
        }

        public Result<(Pointer, Value)> GetPointerAndValue(string key)
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return new InvalidOperation<(Pointer, Value)>($"There are no values in pointers. Method must be called on a ValuePointer (i.e. Md with Level = 0). Key {key}.");
                case MdType.Values:
                    if (_valueFields.ContainsKey(key))
                    {
                        var value = _valueFields[key];
                        return Result.OK((new Pointer
                        {
                            XORAddress = this.XORAddress,
                            MdKey = key,
                            ValueType = value.ValueType
                        }, value));
                    }
                    else
                        return new KeyNotFound<(Pointer, Value)>($"Key: {key}");
                default:
                    return new ArgumentOutOfRange<(Pointer, Value)>(nameof(Type));
            }
        }

        public Result<Pointer> Add(Pointer pointer)
        {
            if (IsFull)
                return new MdOutOfEntriesError<Pointer>($"Filled: {Metadata.Count}/{Metadata.Capacity}");
            var index = Count + 1;
            _pointerFields[index] = pointer;
            _metadata.IncrementCount();
            return Result.OK(pointer);
        }

        public Result<Pointer> Set(string key, Value value)
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return new InvalidOperation<Pointer>($"Cannot set values directly on pointers. Key {key}, value type {value.ValueType}");
                case MdType.Values:
                    _valueFields[key] = value;
                    return Result.OK(new Pointer
                    {
                        XORAddress = this.XORAddress,
                        MdKey = key,
                        ValueType = value.ValueType
                    });
                default:
                    return new ArgumentOutOfRange<Pointer>(nameof(Type));
            }
        }

        public Result<Pointer> Delete(string key)
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
                        XORAddress = this.XORAddress,
                        MdKey = key,
                        ValueType = value.ValueType
                    });
                default:
                    return new ArgumentOutOfRange<Pointer>(nameof(Type));
            }
        }

        // It will return the direct pointer to the stored value
        // which makes it readily available for indexing at higher levels.
        public Result<Pointer> Add(string key, Value value)
        {
            if (IsFull)
                return new MdOutOfEntriesError<Pointer>($"Filled: {Metadata.Count}/{Metadata.Capacity}");
            switch (Type)
            {
                case MdType.Pointers:
                    if (Count == 0)
                        return ExpandLevel(key, value);

                    var target = Locate(_pointerFields[Count].XORAddress);
                    if (target.IsFull) 
                        return ExpandLevel(key, value);

                    return target.Add(key, value);
                case MdType.Values:
                    if (_valueFields.ContainsKey(key))
                        return new ValueAlreadyExists<Pointer>($"Key: {key}.");

                    _valueFields[key] = value;
                    _metadata.IncrementCount();
                    return Result.OK(new Pointer
                    {
                        XORAddress = this.XORAddress,
                        MdKey = key,
                        ValueType = value.ValueType
                    });
                default:
                    return new ArgumentOutOfRange<Pointer>(nameof(Type));
            }
        }

        Result<Pointer> ExpandLevel(string key, Value value)
        {
            if (Level == 0)
                return new ArgumentOutOfRange<Pointer>(nameof(Level));

            var md = Md.Create(Level - 1);
            var leafPointer = md.Add(key, value);
            if (!leafPointer.HasValue)
                return leafPointer;

            switch (md.Type)
            {
                case MdType.Pointers: // i.e. we have still not reached the end of the tree
                    Add(new Pointer
                    {
                        XORAddress = md.XORAddress,
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
