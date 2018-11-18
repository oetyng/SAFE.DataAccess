using SafeApp;
using SafeApp.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess.Network
{
    public class MdOps : NetworkDataOps, IMd
    {
        protected const string METADATA_KEY = "metadata";
        protected static readonly List<byte> METADATA_KEY_BYTES;

        protected MDataInfo _mdInfo;
        protected MdMetadata _metadata;

        public const int MAX_COUNT = MdMetadata.Capacity;

        public MdType Type => _metadata.Type;
        public int Count => _metadata.Count;
        public int Level => _metadata.Level;
        public bool IsFull => _metadata.Count > MdMetadata.Capacity;
        public byte[] XORAddress => _metadata.XORAddress;
        public TMeta GetMetaValue<TMeta>(string key) => _metadata.Get<TMeta>(key);

        static MdOps()
        {
            METADATA_KEY_BYTES = METADATA_KEY.ToUtfBytes();
        }

        public MdOps(MDataInfo mdInfo, Session session)
            : base(session)
        {
            _mdInfo = mdInfo;
        }

        public Task Initialize(int level)
        {
            return GetOrAddMetadata(level);
        }

        // 50 % converted
        // level 0 gives new leaf 
        public static async Task<IMd> CreateAsync(int level, Session session)
        {
            //session.MDataInfoActions.
            var newMd = new MdOps(default(MDataInfo), session);
            await newMd.Initialize(level).ConfigureAwait(false);
            return newMd;
        }

        // 50 % converted
        public static async Task<IMd> LocateAsync(byte[] xorAddress, Session session)
        {
            // try find on network
            if (false)// if not found, create with level 0
                await CreateAsync(0, session).ConfigureAwait(false);
            //session.MDataInfoActions.
            var newMd = new MdOps(default(MDataInfo), session);
            await newMd.Initialize(level: 0).ConfigureAwait(false);
            return newMd;
        }

        public async Task<long> GetEntryVersionAsync(string key)
        {
            try
            {
                var entry = await _session.MData.GetValueAsync(_mdInfo, key.ToUtfBytes()).ConfigureAwait(false);
                return (long)entry.Item2;
            }
            catch
            {
                return -1;
            }
        }

        public async Task<bool> ContainsKeyAsync(string key)
        {
            try
            {
                var keyEntries = await _session.MData.ListKeysAsync(_mdInfo).ConfigureAwait(false);
                var keys = keyEntries.Select(c => c.Val);
                var keyBytes = key.ToUtfBytes();
                return keys.Any(c => c.SequenceEqual(keyBytes));
            }
            catch (FfiException)
            {
                throw; // todo: fix correct return value
            }
        }

        public async Task<List<string>> GetKeysAsync()
        {
            try
            {
                var keyEntries = await _session.MData.ListKeysAsync(_mdInfo).ConfigureAwait(false);
                var keys = keyEntries.Select(c => c.Val.ToUtfString()).ToList();
                return keys;
            }
            catch (FfiException)
            {
                throw; // todo: fix correct return value
            }
        }

        // Converted
        public async Task<Result<Value>> GetValueAsync(string key)
        {
            try
            {
                switch (Type)
                {
                    case MdType.Pointers:
                        return new InvalidOperation<Value>($"There are no values in pointers. Method must be called on a ValuePointer (i.e. Md with Level = 0). Key {key}.");
                    case MdType.Values:
                        var mdRef = await _session.MData.GetValueAsync(_mdInfo, key.ToUtfBytes()).ConfigureAwait(false);
                        if (mdRef.Item1.Count == 0) // beware of this, is an empty list always the same as a deleted value?
                            return new ValueDeleted<Value>($"Key: {key}.");

                        var json = mdRef.Item1.ToUtfString();
                        if (!json.TryParse(out Value item)) // beware of this, the type parsed must have proper property validations for this to work (Like [JsonRequired])
                            return new DeserializationError<Value>();
                        return Result.OK(item);
                    default:
                        return new ArgumentOutOfRange<Value>(nameof(Type));
                }
            }
            catch (FfiException ex)
            {
                if (ex.ErrorCode != -106)
                    throw;
                return new KeyNotFound<Value>($"Key: {key}.");
            }
        }

        // Added for conversion
        async Task<Result<Pointer>> GetPointerAsync(string key)
        {
            try
            {
                switch (Type)
                {
                    case MdType.Pointers:
                        var mdRef = await _session.MData.GetValueAsync(_mdInfo, key.ToUtfBytes()).ConfigureAwait(false);
                        if (mdRef.Item1.Count == 0) // beware of this, is an empty list always the same as a deleted value?
                            return new ValueDeleted<Pointer>($"Key: {key}.");

                        var json = mdRef.Item1.ToUtfString();
                        if (!json.TryParse(out Pointer item)) // beware of this, the type parsed must have proper property validations for this to work (Like [JsonRequired])
                            return new DeserializationError<Pointer>();
                        return Result.OK(item);
                    case MdType.Values:
                        return new InvalidOperation<Pointer>($"There are no pointers in value mds. Method must be called on a Pointer (i.e. Md with Level > 0). Key {key}.");
                        
                    default:
                        return new ArgumentOutOfRange<Pointer>(nameof(Type));
                }
            }
            catch (FfiException ex)
            {
                if (ex.ErrorCode != -106)
                    throw;
                return new KeyNotFound<Pointer>($"Key: {key}.");
            }
        }

        // Converted
        public async Task<Result<(Pointer, Value)>> GetPointerAndValueAsync(string key)
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return new InvalidOperation<(Pointer, Value)>($"There are no values in pointers. Method must be called on a ValuePointer (i.e. Md with Level = 0). Key {key}.");
                case MdType.Values:
                    if (await ContainsKeyAsync(key))
                    {
                        var valueResult = await GetValueAsync(key).ConfigureAwait(false);
                        if (!valueResult.HasValue)
                            return Result.Fail<(Pointer, Value)>(valueResult.ErrorCode.Value, valueResult.ErrorMsg);
                        var value = valueResult.Value;
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

        // 50 % converted
        public async Task<IEnumerable<Value>> GetAllValuesAsync()
        {
            try
            {
                var bag = new ConcurrentBag<Value>();
                var values = await _session.MData.ListValuesAsync(_mdInfo).ConfigureAwait(false);

                switch (Type)
                {
                    case MdType.Pointers:
                        var pointerBag = new ConcurrentBag<Pointer>();
                        Parallel.ForEach(values, val =>
                        {
                            if (val.Content.ToUtfString().TryParse(out Pointer result))
                                pointerBag.Add(result);
                        });
                        // from pointerBag get regs to mds
                        // pointerBag
                        //    .Select(c => Locate(c.XORAddress))
                        //    .SelectMany(c => c.GetAllValues());
                        // add to bag
                        return bag;
                    case MdType.Values:
                        Parallel.ForEach(values, val =>
                        {
                            if (val.Content.ToUtfString().TryParse(out Value result))
                                bag.Add(result);
                        });
                        return bag;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(Type));
                }
            }
            catch (FfiException ex)
            {
                //if (ex.ErrorCode != -106) // does not make sense to check for key not found error here
                //    throw;
                throw;
            }
        }

        // Converted
        public async Task<IEnumerable<(Pointer, Value)>> GetAllPointerValuesAsync()
        {
            switch (Type)
            {
                case MdType.Pointers:
                    var pointerTasks = (await GetAllPointersAsync().ConfigureAwait(false))
                        .Select(c => LocateAsync(c.XORAddress, _session));
                    var pointerValuesTasks = (await Task.WhenAll(pointerTasks).ConfigureAwait(false))
                        .Select(c => c.GetAllPointerValuesAsync());
                    return (await Task.WhenAll(pointerValuesTasks).ConfigureAwait(false))
                        .SelectMany(c => c);
                case MdType.Values:
                    //return (await GetAllValuesAsync())
                    //    .Where(c => c.ValueType != typeof(MdMetadata).Name)
                    //    .Select(c => (new Pointer
                    //    {
                    //        XORAddress = this.XORAddress,
                    //        MdKey = c.Key, // We do not have the key here, unfortunately..
                    //        ValueType = c.ValueType
                    //    }, c));

                    var keys = await GetKeysAsync().ConfigureAwait(false); ;
                    var pairs = new ConcurrentDictionary<string, Value>();
                    var valueTasks = keys.Select(async c =>
                    {
                        var val = await GetValueAsync(c).ConfigureAwait(false); ;
                        if (val.HasValue)
                            pairs[c] = val.Value;
                    });
                    await Task.WhenAll(valueTasks).ConfigureAwait(false); ;
                    return pairs
                        .Where(c => c.Value.ValueType != typeof(MdMetadata).Name)
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

        // Added for conversion
        async Task<IEnumerable<Pointer>> GetAllPointersAsync()
        {
            try
            {
                var pointerBag = new ConcurrentBag<Pointer>();
                var values = await _session.MData.ListValuesAsync(_mdInfo).ConfigureAwait(false);

                switch (Type)
                {
                    case MdType.Pointers:
                        Parallel.ForEach(values, val =>
                        {
                            if (val.Content.ToUtfString().TryParse(out Pointer result))
                                pointerBag.Add(result);
                        });
                        return pointerBag;
                    case MdType.Values:
                        throw new InvalidOperationException("Pointers can only be fetched in Pointer type Mds (i.e. Level > 0).");
                    default:
                        throw new ArgumentOutOfRangeException(nameof(Type));
                }
            }
            catch (FfiException ex)
            {
                //if (ex.ErrorCode != -106) // does not make sense to check for key not found error here
                //    throw;
                throw;
            }
        }

        // Converted
        // Adds if not exists
        // It will return the direct pointer to the stored value
        // which makes it readily available for indexing at higher levels.
        public async Task<Result<Pointer>> AddAsync(string key, Value value)
        {
            if (IsFull)
                return new MdOutOfEntriesError<Pointer>($"Filled: {Count}/{MdMetadata.Capacity}");
            
            try
            {
                switch (Type)
                {
                    case MdType.Pointers:
                        if (Count == 0)
                            return await ExpandLevelAsync(key, value).ConfigureAwait(false);

                        var pointer = await GetPointerAsync(Count.ToString()).ConfigureAwait(false);
                        if (!pointer.HasValue)
                            return pointer;

                        var target = await LocateAsync(pointer.Value.XORAddress, _session).ConfigureAwait(false);
                        if (target.IsFull)
                            return await ExpandLevelAsync(key, value).ConfigureAwait(false);

                        return await target.AddAsync(key, value).ConfigureAwait(false);
                    case MdType.Values:
                        if (await ContainsKeyAsync(key).ConfigureAwait(false))
                            return new ValueAlreadyExists<Pointer>($"Key: {key}.");

                        await AddObjectAsync(key, value).ConfigureAwait(false);

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
            catch (FfiException ex)
            {
                // if ErrorCode == ...
                return new ValueAlreadyExists<Pointer>(ex.Message);
                // else throw;
            }
        }

        // Added for conversion
        async Task AddObjectAsync(string key, object value)
        {
            using (var entryActionsH = await _session.MDataEntryActions.NewAsync().ConfigureAwait(false))
            {
                // insert value
                var insertObj = new Dictionary<string, object>
                {
                    { key, value }
                };
                await InsertEntriesAsync(entryActionsH, insertObj).ConfigureAwait(false);

                // update metadata
                var meta = GetCountBumpedClone(); // clone and bump
                var updateObj = new Dictionary<string, (object, ulong)>
                {
                    { METADATA_KEY, (meta, meta.MetadataVersion) }
                };
                await UpdateEntriesAsync(entryActionsH, updateObj).ConfigureAwait(false);

                // commit
                await CommitEntryMutationAsync(_mdInfo, entryActionsH).ConfigureAwait(false);
                _metadata = meta; // if commit is successful, update our cached meta with bumped metadata instance
            }
        }

        // Converted
        // Adds or overwrites
        public async Task<Result<Pointer>> SetAsync(string key, Value value, long expectedVersion = -1)
        {
            var keyBytes = key.ToUtfBytes();
            ulong version = 0;

            try
            {
                switch (Type)
                {
                    case MdType.Pointers:
                        return new InvalidOperation<Pointer>($"Cannot set values directly on pointers. Key {key}, value type {value.ValueType}");
                    case MdType.Values:
                        var mdRef = await _session.MData.GetValueAsync(_mdInfo, keyBytes).ConfigureAwait(false);
                        version = mdRef.Item2;
                        if (0 > expectedVersion || version != (ulong)expectedVersion)
                            return new VersionMismatch<Pointer>($"Expected {expectedVersion}, but found {version}.");
                        break;
                    default:
                        return new ArgumentOutOfRange<Pointer>(nameof(Type));
                }
            }
            catch // catch only the one where key is missing
            {
                if (expectedVersion > -1)
                    return new VersionMismatch<Pointer>($"Expected {expectedVersion}, but key is missing.");
                return await AddAsync(key, value).ConfigureAwait(false);
            }

            try
            {
                using (var entryActionsH = await _session.MDataEntryActions.NewAsync().ConfigureAwait(false))
                {
                    // update value and update metadata
                    var meta = GetCountBumpedClone(); // clone and bump
                    var updateObj = new Dictionary<string, (object, ulong)>
                    {
                        { key, (value, version + 1) },
                        { METADATA_KEY, (meta, meta.MetadataVersion) }
                    };
                    await UpdateEntriesAsync(entryActionsH, updateObj).ConfigureAwait(false);
                    await CommitEntryMutationAsync(_mdInfo, entryActionsH).ConfigureAwait(false);
                    _metadata = meta; // if commit is successful, update our cached meta with bumped metadata instance

                    return Result.OK(new Pointer
                    {
                        XORAddress = this.XORAddress,
                        MdKey = key,
                        ValueType = value.ValueType
                    });
                }
            }
            catch (FfiException ex)
            {
                return Result.Fail<Pointer>(-999, ex.Message); // todo: fix correct error type
            }
        }

        // Converted
        // Removes if exists, else throws
        public async Task<Result<Pointer>> DeleteAsync(string key)
        {
            try
            {
                switch (Type)
                {
                    case MdType.Pointers:
                        throw new NotImplementedException("hmm...");
                    case MdType.Values:
                        if (!await ContainsKeyAsync(key).ConfigureAwait(false))
                            return new KeyNotFound<Pointer>($"Key: {key}");
                        
                        var keyBytes = key.ToUtfBytes();
                        var mdRef = await _session.MData.GetValueAsync(_mdInfo, keyBytes).ConfigureAwait(false);

                        using (var entryActionsH = await _session.MDataEntryActions.NewAsync().ConfigureAwait(false))
                        {
                            // delete
                            var deleteObj = new Dictionary<string, ulong>
                            {
                                { key, mdRef.Item2 + 1 }
                            };
                            await DeleteEntriesAsync(entryActionsH, deleteObj).ConfigureAwait(false);

                            // update metadata
                            var meta = GetCountDecreasedClone(); // clone and bump
                            var updateObj = new Dictionary<string, (object, ulong)>
                            {
                                { METADATA_KEY, (meta, meta.MetadataVersion) }
                            };
                            await UpdateEntriesAsync(entryActionsH, updateObj).ConfigureAwait(false);

                            // commit
                            await CommitEntryMutationAsync(_mdInfo, entryActionsH).ConfigureAwait(false);
                            _metadata = meta; // if commit is successful, update our cached meta with bumped metadata instance
                        }

                        var json = mdRef.Item1.ToUtfString();
                        if (!json.TryParse(out Value value)) // beware of this, the type parsed must have proper property validations for this to work (Like [JsonRequired])
                            return new DeserializationError<Pointer>();

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
            catch (FfiException ex)
            {
                // if errorcode = ..
                //return ;
                // else..
                throw;
            }
        }

        // Converted
        public async Task<Result<Pointer>> AddAsync(Pointer pointer)
        {
            if (IsFull)
                return new MdOutOfEntriesError<Pointer>($"Filled: {Count}/{MdMetadata.Capacity}");
            if (Type == MdType.Values)
                return new InvalidOperation<Pointer>("Pointers can only be added in Pointer type Mds (i.e. Level > 0).");
            var index = (Count + 1).ToString();
            await AddObjectAsync(index, pointer);
            //_pointerFields[index] = pointer;
            //_metadata.IncrementCount();
            return Result.OK(pointer);
        }


        // Creates if it doesn't exist
        async Task GetOrAddMetadata(int level)
        {
            if (_metadata != null)
                return;
            var keys = await _session.MData.ListKeysAsync(_mdInfo).ConfigureAwait(false);
            if (keys.Any(c => c.Val.SequenceEqual(METADATA_KEY_BYTES)))
            {
                var metaMD = await _session.MData.GetValueAsync(_mdInfo, METADATA_KEY_BYTES).ConfigureAwait(false);
                _metadata = metaMD.Item1.ToUtfString().Parse<MdMetadata>();
                return;
            }

            //var meta = Activator.CreateInstance<T>(new object[] { level });
            var meta = new MdMetadata(level)
            {
                XORAddress = _mdInfo.Name
            };

            // 
            var value = new Value
            {
                Payload = meta.Json(),
                ValueType = typeof(MdMetadata).Name
            };

            var insertObj = new Dictionary<string, object>
            {
                { METADATA_KEY, meta }
            };
            using (var entryActionsH = await _session.MDataEntryActions.NewAsync().ConfigureAwait(false))
            {
                await InsertEntriesAsync(entryActionsH, insertObj).ConfigureAwait(false);
                await CommitEntryMutationAsync(_mdInfo, entryActionsH).ConfigureAwait(false);
            }
            _metadata = meta;
        }

        public async Task SetMetadata(string key, object value)
        {
            var meta = _metadata.Clone<MdMetadata>();
            meta.Set(key, value);

            try
            {
                await Update(meta).ConfigureAwait(false);
            }
            catch (FfiException ex) // optimistic concurrency
            {
                if (ex.ErrorCode != -107)
                    throw;

                var metaMD = await _session.MData.GetValueAsync(_mdInfo, METADATA_KEY_BYTES).ConfigureAwait(false);
                _metadata = metaMD.Item1.ToUtfString().Parse<MdMetadata>();

                meta = _metadata.Clone<MdMetadata>();
                meta.Set(key, value);

                await Update(meta).ConfigureAwait(false);
            }
        }

        async Task Update(MdMetadata meta)
        {
            meta.MetadataVersion++; // increase version

            var insertObj = new Dictionary<string, (object, ulong)>
            {
                { METADATA_KEY, (meta, meta.MetadataVersion) }
            };
            using (var entryActionsH = await _session.MDataEntryActions.NewAsync().ConfigureAwait(false))
            {
                await UpdateEntriesAsync(entryActionsH, insertObj).ConfigureAwait(false);
                await CommitEntryMutationAsync(_mdInfo, entryActionsH).ConfigureAwait(false);
            }
            _metadata = meta;
        }

        public async Task<Result<object>> TryGetMetadata(string key, bool fromCache = false)
        {
            if (fromCache)
                return Result.OK(_metadata.Get(key));

            try
            {
                var metaMD = await _session.MData.GetValueAsync(_mdInfo, METADATA_KEY_BYTES).ConfigureAwait(false);
                _metadata = metaMD.Item1.ToUtfString().Parse<MdMetadata>();
                if (!_metadata.ContainsKey(key))
                    return new KeyNotFound<object>();
                return Result.OK(_metadata.Get(key));
            }
            catch (FfiException ex)
            {
                if (ex.ErrorCode != -103)
                    throw;
                return new KeyNotFound<object>();
            }
        }

        // so we don't get left with an updated cache after failed commit
        protected MdMetadata GetCountBumpedClone()
        {
            var meta = _metadata.Clone<MdMetadata>();
            meta.IncrementCount();
            meta.IncrementVersion(); // increase version count
            return meta;
        }

        // so we don't get left with an updated cache after failed commit
        protected MdMetadata GetCountDecreasedClone()
        {
            var meta = _metadata.Clone<MdMetadata>();
            meta.DecrementCount();
            meta.DecrementVersion(); // increase version count
            return meta;
        }

        // Added for conversion
        async Task<Result<Pointer>> ExpandLevelAsync(string key, Value value)
        {
            if (Level == 0)
                return new ArgumentOutOfRange<Pointer>(nameof(Level));

            var md = await CreateAsync(Level - 1, _session).ConfigureAwait(false);
            var leafPointer = await md.AddAsync(key, value).ConfigureAwait(false);
            if (!leafPointer.HasValue)
                return leafPointer;

            switch (md.Type)
            {
                case MdType.Pointers: // i.e. we have still not reached the end of the tree
                    await AddAsync(new Pointer
                    {
                        XORAddress = md.XORAddress,
                        ValueType = typeof(Pointer).Name
                    }).ConfigureAwait(false);
                    break;
                case MdType.Values:  // i.e. we are now right above leaf level
                    await AddAsync(leafPointer.Value).ConfigureAwait(false);
                    break;
                default:
                    return new ArgumentOutOfRange<Pointer>(nameof(md.Type));
            }

            return leafPointer;
        }
    }
}
