using SAFE.DataAccess.Factories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    public class Database
    {
        IMd _info;
        TypeStoreInfo _typeInfo;
        protected Dictionary<string, byte[]> _dataTreeAddresses = new Dictionary<string, byte[]>();
        protected Dictionary<string, DataTree> _dataTreeCache = new Dictionary<string, DataTree>();

        IIndexer _indexer;

        protected Database(IMd info, TypeStoreInfo typeInfo, IIndexer indexer)
        {
            _info = info;
            _typeInfo = typeInfo;
            _indexer = indexer;
        }

        public static async Task<Result<Database>> GetOrAddAsync(string id, IIndexer indexer)
        {
            var dbInfoMd = await MdAccess.LocateAsync(System.Text.Encoding.UTF8.GetBytes(id)).ConfigureAwait(false);
            var typeInfo = await TypeStoreInfoFactory.GetOrAddTypeStoreAsync(dbInfoMd, id).ConfigureAwait(false);

            var db = new Database(dbInfoMd, typeInfo, indexer);

            var typeStores = await typeInfo.GetAllAsync().ConfigureAwait(false);
            db._dataTreeAddresses = typeStores
                .ToDictionary(c => c.Item1, c => c.Item2);

            return Result.OK(db);
        }

        public async Task<Result<T>> FindByKeyAsync<T>(string key)
        {
            var type = typeof(T).Name;
            var indexKey = $"{type}/{key}";
            var (data, errors) = await _indexer.GetAllValuesAsync<T>(indexKey).ConfigureAwait(false);
            var list = data.ToList();
            if (list.Count == 0)
                return new KeyNotFound<T>(string.Join(',', errors));
            if (list.Count > 1)
                return new MultipleResults<T>($"Expected 1 result, found: {list.Count}.");
            return Result.OK(list.Single());
        }

        public Task<(IEnumerable<T> data, IEnumerable<string> errors)> FindAsync<T>(string whereProperty, object isValue)
        {
            var indexKey = $"{whereProperty}/{isValue}";
            return _indexer.GetAllValuesAsync<T>(indexKey);
        }

        // Scan. Not recommended to be used with any larger amounts of data.
        public async Task<IEnumerable<T>> GetAllAsync<T>()
        {
            var type = typeof(T).Name;
            if (!_dataTreeCache.ContainsKey(type))
                await LoadStoreAsync(type).ConfigureAwait(false);

            var data = (await _dataTreeCache[type]
                .GetAllValuesAsync().ConfigureAwait(false))
                .Select(c => c.Payload.Parse<T>());

            return data;
        }

        public async Task CreateIndex<T>(string[] propertyPath)
        {
            var type = typeof(T).Name;

            if (!_dataTreeCache.ContainsKey(type))
                await LoadStoreAsync(type).ConfigureAwait(false);

            var pointerValues = await _dataTreeCache[type]
                .GetAllPointerValuesAsync()
                .ConfigureAwait(false);

            await _indexer.CreateIndexAsync<T>(propertyPath, pointerValues).ConfigureAwait(false);
        }

        public Task<Result<Pointer>> AddAsync<T>(string key, T data)
        {
            return AddAsync(key, (object)data);
        }

        public async Task<Result<Pointer>> AddAsync(string key, object data)
        {
            var type = data.GetType().Name;

            if (!_dataTreeAddresses.ContainsKey(type))
                await AddStoreAsync(type).ConfigureAwait(false);

            var value = new Value
            {
                Payload = data.Json(),
                ValueType = type
            };
            var pointer = await _dataTreeCache[type].AddAsync(key, value).ConfigureAwait(false);

            if (!pointer.HasValue)
                return pointer;

            await IndexOnKey(type, key, pointer.Value).ConfigureAwait(false);
            await TryIndexProperties(data, pointer.Value).ConfigureAwait(false);

            return pointer;
        }

        public async Task<Result<T>> Update<T>(string key, T newValue)
        {
            var findResult = await FindValuePointerByKeyAsync<T>(key).ConfigureAwait(false);
            if (!findResult.HasValue)
                return Result.Fail<T>(findResult.ErrorCode.Value, findResult.ErrorMsg);

            // modify md key
            var md = await MdAccess.LocateAsync(findResult.Value.XORAddress).ConfigureAwait(false);
            var setResult = await md.SetAsync(key, new Value
            {
                Payload = newValue.Json(),
                ValueType = typeof(T).Name
            }).ConfigureAwait(false);
            if (setResult.HasValue)
                return Result.OK(newValue);
            else
                return Result.Fail<T>(setResult.ErrorCode.Value, setResult.ErrorMsg);

            // TODO: re-index
        }

        public async Task<Result<Pointer>> Delete<T>(string key) // What to return? Result<(Pointer, Value)>, Result<Pointer>, Result<Value>, Result<T>
        {
            var findResult = await FindValuePointerByKeyAsync<T>(key).ConfigureAwait(false);
            if (!findResult.HasValue)
                return findResult;

            var md = await MdAccess.LocateAsync(findResult.Value.XORAddress).ConfigureAwait(false);
            var deleteResult = await md.DeleteAsync(key).ConfigureAwait(false);
            return deleteResult;

            // TODO: re-index
        }

        async Task<Result<Pointer>> FindValuePointerByKeyAsync<T>(string key)
        {
            var type = typeof(T).Name;
            var indexKey = $"{type}/{key}";
            var (data, errors) = await _indexer.GetAllPointersWithValuesAsync(indexKey).ConfigureAwait(false);
            var list = data.ToList();
            if (list.Count == 0)
                return new KeyNotFound<Pointer>(string.Join(',', errors));
            if (list.Count > 1)
                return new MultipleResults<Pointer>($"Expected 1 result, found: {list.Count}.");
            return Result.OK(list.Single().Item1);
        }

        // 1. Index on the type and key
        Task IndexOnKey(string type, string key, Pointer valuePointer)
        {
            var indexKey = $"{type}/{key}";
            return _indexer.IndexAsync(indexKey, valuePointer);
        }

        // 2. Find any other indices
        Task TryIndexProperties(object data, Pointer valuePointer)
        {
            return _indexer.TryIndexAsync(data, valuePointer);
        }

        Task AddStoreAsync<T>()
        {
            var type = typeof(T).Name;
            return AddStoreAsync(type);
        }

        protected async Task AddStoreAsync(string type)
        {
            if (_dataTreeAddresses.ContainsKey(type))
                return;

            Task onHeadChange(byte[] newXOR) => UpdateTypeStores(type, newXOR);
            
            var dataTree = await DataTreeFactory.CreateAsync(onHeadChange).ConfigureAwait(false);
            _dataTreeCache[type] = dataTree;
            _dataTreeAddresses[type] = dataTree.XORAddress;
            await _typeInfo.AddAsync(type, dataTree.XORAddress).ConfigureAwait(false);
        }

        protected async Task LoadStoreAsync(string type)
        {
            if (!_dataTreeAddresses.ContainsKey(type))
                throw new InvalidOperationException($"Store does not exist! {type}");

            Task onHeadChange(byte[] newXOR) => UpdateTypeStores(type, newXOR);
            var head = await MdAccess.LocateAsync(_dataTreeAddresses[type]).ConfigureAwait(false);
            var dataTree = new DataTree(head, onHeadChange);
            _dataTreeCache[type] = dataTree;
        }

        async Task UpdateTypeStores(string type, byte[] XORAddress)
        {
            await _typeInfo.UpdateAsync(type, XORAddress).ConfigureAwait(false);
            _dataTreeAddresses[type] = XORAddress;
        }
    }
}
