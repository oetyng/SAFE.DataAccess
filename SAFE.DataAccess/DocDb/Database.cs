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
        protected Dictionary<string, MdLocator> _dataTreeAddresses = new Dictionary<string, MdLocator>();
        protected Dictionary<string, DataTree> _dataTreeCache = new Dictionary<string, DataTree>();

        IIndexer _indexer;

        protected Database(IMd info, TypeStoreInfo typeInfo, IIndexer indexer)
        {
            _info = info;
            _typeInfo = typeInfo;
            _indexer = indexer;
        }

        public static async Task<Result<Database>> GetOrAddAsync(MdHead mdHead, IIndexer indexer)
        {
            var typeInfo = await TypeStoreInfoFactory.GetOrAddTypeStoreAsync(mdHead.Md, mdHead.Id).ConfigureAwait(false);

            var db = new Database(mdHead.Md, typeInfo, indexer);

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

            var value = new StoredValue(data);
            var pointer = await _dataTreeCache[type]
                .AddAsync(key, value)
                .ConfigureAwait(false);

            if (!pointer.HasValue)
                return pointer;

            await IndexOnKey(type, key, pointer.Value).ConfigureAwait(false);
            //await TryIndexProperties(data, pointer.Value).ConfigureAwait(false);

            return pointer;
        }

        public async Task<Result<T>> Update<T>(string key, T newValue)
        {
            var findResult = await FindValuePointerByKeyAsync<T>(key).ConfigureAwait(false);
            if (!findResult.HasValue)
                return Result.Fail<T>(findResult.ErrorCode.Value, findResult.ErrorMsg);

            // modify md key
            var mdResult = await MdAccess.LocateAsync(findResult.Value.MdLocator).ConfigureAwait(false);
            if (!mdResult.HasValue)
                return Result.Fail<T>(mdResult.ErrorCode.Value, mdResult.ErrorMsg);

            var setResult = await mdResult.Value.SetAsync(key, new StoredValue(newValue)).ConfigureAwait(false);
            if (setResult.HasValue)
                return Result.OK(newValue);
            else
                return Result.Fail<T>(setResult.ErrorCode.Value, setResult.ErrorMsg);

            // TODO: re-index
            //_indexer.Delete(key);
            //IndexOnKey(typeof(T).Name, key, setResult.Value);
        }

        public async Task<Result<Pointer>> Delete<T>(string key) // What to return? Result<(Pointer, Value)>, Result<Pointer>, Result<Value>, Result<T>
        {
            var findResult = await FindValuePointerByKeyAsync<T>(key).ConfigureAwait(false);
            if (!findResult.HasValue)
                return findResult;

            var mdResult = await MdAccess.LocateAsync(findResult.Value.MdLocator).ConfigureAwait(false);
            if (!mdResult.HasValue)
                return Result.Fail<Pointer>(mdResult.ErrorCode.Value, mdResult.ErrorMsg);

            var deleteResult = await mdResult.Value.DeleteAsync(key).ConfigureAwait(false);
            return deleteResult;

            // TODO: re-index
            //_indexer.Delete(key);
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

            Task onHeadChange(MdLocator newLocation) => UpdateTypeStores(type, newLocation);
            
            var dataTree = await DataTreeFactory.CreateAsync(onHeadChange).ConfigureAwait(false);
            _dataTreeCache[type] = dataTree;
            _dataTreeAddresses[type] = dataTree.MdLocator;
            await _typeInfo.AddAsync(type, dataTree.MdLocator).ConfigureAwait(false);
        }

        protected async Task LoadStoreAsync(string type)
        {
            if (!_dataTreeAddresses.ContainsKey(type))
                throw new InvalidOperationException($"Store does not exist! {type}");

            Task onHeadChange(MdLocator newXOR) => UpdateTypeStores(type, newXOR);
            var headResult = await MdAccess.LocateAsync(_dataTreeAddresses[type]).ConfigureAwait(false);
            if (!headResult.HasValue)
                throw new Exception($"Error code: {headResult.ErrorCode.Value}. {headResult.ErrorMsg}");
            var dataTree = new DataTree(headResult.Value, onHeadChange);
            _dataTreeCache[type] = dataTree;
        }

        async Task UpdateTypeStores(string type, MdLocator location)
        {
            await _typeInfo.UpdateAsync(type, location).ConfigureAwait(false);
            _dataTreeAddresses[type] = location;
        }
    }
}
