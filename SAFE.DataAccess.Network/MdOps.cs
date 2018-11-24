using SafeApp;
using SafeApp.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess.Network
{
    public sealed class MdOps : NetworkDataOps, IMd
    {
        static readonly string METADATA_KEY = 0.ToString();
        static readonly List<byte> METADATA_KEY_BYTES = METADATA_KEY.ToUtfBytes();

        MDataInfo _mdInfo;
        int _count;
        MdType _type;
        int _level;

        public MdType Type => _type;
        public int Count => _count;
        public int Level => _level;
        public bool IsFull => _count >= MdMetadata.Capacity;
        public int Capacity => MdMetadata.Capacity;
        public MdLocator MdLocator => new MdLocator(_mdInfo.Name, _mdInfo.TypeTag);

        public MdOps(MDataInfo mdInfo, Session session)
            : base(session)
        {
            _mdInfo = mdInfo;
        }

        // level 0 gives new leaf 
        public Task Initialize(int level)
        {
            return GetOrAddMetadata(level);
        }

        public static async Task<Result<IMd>> LocateAsync(MdLocator location, NetworkDataOps networkDataOp)
        {
            var mdResult = await networkDataOp.LocatePublicMd(location.XORName, location.TypeTag);
            if (!mdResult.HasValue)
                return new KeyNotFound<IMd>($"Could not locate md: {location.TypeTag}, {location.XORName}");

            var mdInfo = mdResult.Value;
            var md = new MdOps(mdInfo, networkDataOp.Session);
            await md.GetOrAddMetadata();
            return Result.OK((IMd)md);
        }

        public static async Task<IMd> CreateNewMdOpsAsync(int level, NetworkDataOps networkDataOps, ulong protocol)
        {
            var session = networkDataOps.Session;

            // Create Permissions
            using (var permissionsHandle = await session.MDataPermissions.NewAsync())
            {
                using (var appSignPkH = await session.Crypto.AppPubSignKeyAsync())
                {
                    await session.MDataPermissions.InsertAsync(permissionsHandle, appSignPkH, networkDataOps.GetFullPermissions());
                }

                var mdInfo = await networkDataOps.CreateEmptyRandomPrivateMd(permissionsHandle, protocol);
                var newMd = new MdOps(mdInfo, session);
                await newMd.Initialize(level).ConfigureAwait(false);
                return newMd;
            }
        }

        public async Task<long> GetEntryVersionAsync(string key)
        {
            try
            {
                var entry = await Session.MData.GetValueAsync(_mdInfo, key.ToUtfBytes()).ConfigureAwait(false);
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
                var keyEntries = await Session.MData.ListKeysAsync(_mdInfo).ConfigureAwait(false);
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
                var keyEntries = await Session.MData.ListKeysAsync(_mdInfo).ConfigureAwait(false);
                var keys = keyEntries.Select(c => c.Val.ToUtfString()).ToList();
                return keys;
            }
            catch (FfiException)
            {
                throw; // todo: fix correct return value
            }
        }

        public async Task<Result<StoredValue>> GetValueAsync(string key)
        {
            try
            {
                switch (Type)
                {
                    case MdType.Pointers:
                        return new InvalidOperation<StoredValue>($"There are no values in pointers. Method must be called on a ValuePointer (i.e. Md with Level = 0). Key {key}.");
                    case MdType.Values:
                        var mdRef = await Session.MData.GetValueAsync(_mdInfo, key.ToUtfBytes()).ConfigureAwait(false);
                        if (mdRef.Item1.Count == 0) // beware of this, is an empty list always the same as a deleted value?
                            return new ValueDeleted<StoredValue>($"Key: {key}.");

                        var json = mdRef.Item1.ToUtfString();
                        if (!json.TryParse(out StoredValue item)) // beware of this, the type parsed must have proper property validations for this to work (Like [JsonRequired])
                            return new DeserializationError<StoredValue>();
                        return Result.OK(item);
                    default:
                        return new ArgumentOutOfRange<StoredValue>(nameof(Type));
                }
            }
            catch (FfiException ex)
            {
                if (ex.ErrorCode != -106)
                    throw;
                return new KeyNotFound<StoredValue>($"Key: {key}.");
            }
        }

        async Task<Result<Pointer>> GetPointerAsync(string key)
        {
            try
            {
                switch (Type)
                {
                    case MdType.Pointers:
                        var mdRef = await Session.MData.GetValueAsync(_mdInfo, key.ToUtfBytes()).ConfigureAwait(false);
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

        public async Task<Result<(Pointer, StoredValue)>> GetPointerAndValueAsync(string key)
        {
            switch (Type)
            {
                case MdType.Pointers:
                    return new InvalidOperation<(Pointer, StoredValue)>($"There are no values in pointers. Method must be called on a ValuePointer (i.e. Md with Level = 0). Key {key}.");
                case MdType.Values:
                    if (await ContainsKeyAsync(key))
                    {
                        var valueResult = await GetValueAsync(key).ConfigureAwait(false);
                        if (!valueResult.HasValue)
                            return Result.Fail<(Pointer, StoredValue)>(valueResult.ErrorCode.Value, valueResult.ErrorMsg);
                        var value = valueResult.Value;
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

        public async Task<IEnumerable<StoredValue>> GetAllValuesAsync()
        {
            try
            {
                var valueBag = new ConcurrentBag<StoredValue>();
                var values = await Session.MData.ListValuesAsync(_mdInfo).ConfigureAwait(false);

                switch (Type)
                {
                    case MdType.Pointers:
                        var pointerBag = new ConcurrentBag<Pointer>();
                        Parallel.ForEach(values, val =>
                        {
                            var couldParse = val.Content.ToUtfString()
                                .TryParse(out Pointer result);
                            if (couldParse && result.ValueType != typeof(MdMetadata).Name)
                                pointerBag.Add(result);
                        });
                        // from pointerBag get regs to mds
                        var pointerTasks = pointerBag
                            .Select(c => LocateAsync(c.MdLocator, new NetworkDataOps(Session)));
                        var pointerValues = (await Task.WhenAll(pointerTasks)
                            .ConfigureAwait(false));
                        var valueTasks = pointerValues
                           .Select(c => c.Value.GetAllValuesAsync());
                        var fetchedValues = (await Task.WhenAll(valueTasks))
                            .SelectMany(c => c);
                        Parallel.ForEach(fetchedValues, val => valueBag.Add(val));
                        return valueBag;
                    case MdType.Values:
                        Parallel.ForEach(values, val =>
                        {
                            var json = val.Content.ToUtfString();
                            if (json.TryParse(out StoredValue result))
                                valueBag.Add(result);
                        });
                        return valueBag.Where(c => c.ValueType != typeof(MdMetadata).Name);
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

        public async Task<IEnumerable<(Pointer, StoredValue)>> GetAllPointerValuesAsync()
        {
            switch (Type)
            {
                case MdType.Pointers:
                    var pointerTasks = (await GetAllPointersAsync().ConfigureAwait(false))
                        .Select(c => LocateAsync(c.MdLocator, new NetworkDataOps(Session)));
                    var pointerValuesTasks = (await Task.WhenAll(pointerTasks).ConfigureAwait(false))
                        .Select(c => c.Value.GetAllPointerValuesAsync());
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

                    var keys = await GetKeysAsync().ConfigureAwait(false);
                    var pairs = new ConcurrentDictionary<string, StoredValue>();
                    var valueTasks = keys.Select(async c =>
                    {
                        var val = await GetValueAsync(c).ConfigureAwait(false);
                        if (val.HasValue)
                            pairs[c] = val.Value;
                    });
                    await Task.WhenAll(valueTasks).ConfigureAwait(false);

                    return pairs
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

        // Added for conversion
        async Task<IEnumerable<Pointer>> GetAllPointersAsync()
        {
            try
            {
                var pointerBag = new ConcurrentBag<Pointer>();
                var values = await Session.MData.ListValuesAsync(_mdInfo).ConfigureAwait(false);

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

        // Adds if not exists
        // It will return the direct pointer to the stored value
        // which makes it readily available for indexing at higher levels.
        public async Task<Result<Pointer>> AddAsync(string key, StoredValue value)
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

                        var targetResult = await LocateAsync(pointer.Value.MdLocator, new NetworkDataOps(Session))
                            .ConfigureAwait(false);
                        if (!targetResult.HasValue)
                            return Result.Fail<Pointer>(targetResult.ErrorCode.Value, targetResult.ErrorMsg);
                        var target = targetResult.Value;
                        if (target.IsFull)
                            return await ExpandLevelAsync(key, value).ConfigureAwait(false);

                        return await target.AddAsync(key, value).ConfigureAwait(false);
                    case MdType.Values:
                        if (await ContainsKeyAsync(key).ConfigureAwait(false))
                            return new ValueAlreadyExists<Pointer>($"Key: {key}.");

                        await AddObjectAsync(key, value).ConfigureAwait(false);

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
            catch (FfiException ex)
            {
                // if ErrorCode == ...
                return new ValueAlreadyExists<Pointer>(ex.Message);
                // else throw;
            }
        }

        async Task AddObjectAsync(string key, object value)
        {
            using (var entryActionsH = await Session.MDataEntryActions.NewAsync().ConfigureAwait(false))
            {
                // insert value
                var insertObj = new Dictionary<string, object>
                {
                    { key, value }
                };
                await InsertEntriesAsync(entryActionsH, insertObj).ConfigureAwait(false);

                // commit
                await CommitEntryMutationAsync(_mdInfo, entryActionsH).ConfigureAwait(false);

                ++_count;
            }
        }

        // Adds or overwrites
        public async Task<Result<Pointer>> SetAsync(string key, StoredValue value, long expectedVersion = -1)
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
                        var mdRef = await Session.MData.GetValueAsync(_mdInfo, keyBytes).ConfigureAwait(false);
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
                using (var entryActionsH = await Session.MDataEntryActions.NewAsync().ConfigureAwait(false))
                {
                    // update value
                    var updateObj = new Dictionary<string, (object, ulong)>
                    {
                        { key, (value, version + 1) },
                    };
                    await UpdateEntriesAsync(entryActionsH, updateObj).ConfigureAwait(false);
                    await CommitEntryMutationAsync(_mdInfo, entryActionsH).ConfigureAwait(false);

                    return Result.OK(new Pointer
                    {
                        MdLocator = this.MdLocator,
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
                        var mdRef = await Session.MData.GetValueAsync(_mdInfo, keyBytes).ConfigureAwait(false);

                        using (var entryActionsH = await Session.MDataEntryActions.NewAsync().ConfigureAwait(false))
                        {
                            // delete
                            var deleteObj = new Dictionary<string, ulong>
                            {
                                { key, mdRef.Item2 + 1 }
                            };
                            await DeleteEntriesAsync(entryActionsH, deleteObj).ConfigureAwait(false);

                            // commit
                            await CommitEntryMutationAsync(_mdInfo, entryActionsH).ConfigureAwait(false);
                        }

                        var json = mdRef.Item1.ToUtfString();
                        if (!json.TryParse(out StoredValue value)) // beware of this, the type parsed must have proper property validations for this to work (Like [JsonRequired])
                            return new DeserializationError<Pointer>();

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
            catch (FfiException ex)
            {
                // if errorcode = ..
                //return ;
                // else..
                throw;
            }
        }

        public async Task<Result<Pointer>> AddAsync(Pointer pointer)
        {
            if (IsFull)
                return new MdOutOfEntriesError<Pointer>($"Filled: {Count}/{MdMetadata.Capacity}");
            if (Type == MdType.Values)
                return new InvalidOperation<Pointer>("Pointers can only be added in Pointer type Mds (i.e. Level > 0).");
            var index = (Count + 1).ToString();
            pointer.MdKey = index;
            await AddObjectAsync(index, pointer);
            return Result.OK(pointer);
        }

        // Creates if it doesn't exist
        async Task GetOrAddMetadata(int level = 0)
        {
            var keys = await Session.MData.ListKeysAsync(_mdInfo).ConfigureAwait(false);
            if (keys.Count > 0)
            {
                _count = keys.Count;
                var metaMD = await Session.MData.GetValueAsync(_mdInfo, METADATA_KEY_BYTES).ConfigureAwait(false);
                _level = metaMD.Item1.ToUtfString().Parse<int>();
                return;
            }

            var insertObj = new Dictionary<string, object>
            {
                { METADATA_KEY, level }
            };
            using (var entryActionsH = await Session.MDataEntryActions.NewAsync().ConfigureAwait(false))
            {
                await InsertEntriesAsync(entryActionsH, insertObj).ConfigureAwait(false);
                await CommitEntryMutationAsync(_mdInfo, entryActionsH).ConfigureAwait(false);
                ++_count;
            }
        }

        async Task<Result<Pointer>> ExpandLevelAsync(string key, StoredValue value)
        {
            if (Level == 0)
                return new ArgumentOutOfRange<Pointer>(nameof(Level));

            var md = await CreateNewMdOps(Level - 1).ConfigureAwait(false);
            var leafPointer = await md.AddAsync(key, value).ConfigureAwait(false);
            if (!leafPointer.HasValue)
                return leafPointer;

            switch (md.Type)
            {
                case MdType.Pointers: // i.e. we have still not reached the end of the tree
                    await AddAsync(new Pointer
                    {
                        MdLocator = md.MdLocator,
                        ValueType = typeof(Pointer).Name
                    }).ConfigureAwait(false);
                    break;
                case MdType.Values:  // i.e. we are now right above leaf level
                    await AddAsync(new Pointer
                    {
                        MdLocator = leafPointer.Value.MdLocator,
                        ValueType = typeof(Pointer).Name
                    }).ConfigureAwait(false);
                    break;
                default:
                    return new ArgumentOutOfRange<Pointer>(nameof(md.Type));
            }

            return leafPointer;
        }

        Task<IMd> CreateNewMdOps(int level)
        {
            return CreateNewMdOpsAsync(level, new NetworkDataOps(Session), DataProtocol.DEFAULT_PROTOCOL);
        }
    }
}
