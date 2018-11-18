using SAFE.DataAccess.Factories;
using System;
using System.Collections.Generic;
using System.Linq;

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

        public static Result<Database> GetOrAdd(string id, IIndexer indexer)
        {
            var dbInfoMd = MdAccess.Locate(System.Text.Encoding.UTF8.GetBytes(id));
            var typeInfo = TypeStoreInfoFactory.GetOrAddTypeStore(dbInfoMd, id);

            var db = new Database(dbInfoMd, typeInfo, indexer);

            var typeStores = typeInfo.GetAll();
            db._dataTreeAddresses = typeStores
                .ToDictionary(c => c.Item1, c => c.Item2);

            return Result.OK(db);
        }

        public Result<T> FindByKey<T>(string key)
        {
            var type = typeof(T).Name;
            var indexKey = $"{type}/{key}";
            var (data, errors) = _indexer.GetAllValues<T>(indexKey);
            var list = data.ToList();
            if (list.Count == 0)
                return new KeyNotFound<T>(string.Join(',', errors));
            if (list.Count > 1)
                return new MultipleResults<T>($"Expected 1 result, found: {list.Count}.");
            return Result.OK(list.Single());
        }

        public (IEnumerable<T> data, IEnumerable<string> errors) Find<T>(string whereProperty, object isValue)
        {
            var indexKey = $"{whereProperty}/{isValue}";
            return _indexer.GetAllValues<T>(indexKey);
        }

        // Scan. Not recommended to be used with any larger amounts of data.
        public IEnumerable<T> GetAll<T>()
        {
            var type = typeof(T).Name;
            if (!_dataTreeCache.ContainsKey(type))
                LoadStore(type);

            var data = _dataTreeCache[type]
                .GetAllValues()
                .Select(c => c.Payload.Parse<T>());

            return data;
        }

        public void CreateIndex<T>(string[] propertyPath)
        {
            var type = typeof(T).Name;

            if (!_dataTreeCache.ContainsKey(type))
                LoadStore(type);

            var pointerValues = _dataTreeCache[type]
                .GetAllPointerValues();

            _indexer.CreateIndex<T>(propertyPath, pointerValues);
        }

        public Result<Pointer> Add<T>(string key, T data)
        {
            return Add(key, (object)data);
        }

        public Result<Pointer> Add(string key, object data)
        {
            var type = data.GetType().Name;

            if (!_dataTreeAddresses.ContainsKey(type))
                AddStore(type);

            var value = new Value
            {
                Payload = data.Json(),
                ValueType = type
            };
            var pointer = _dataTreeCache[type].Add(key, value);

            if (!pointer.HasValue)
                return pointer;

            IndexOnKey(type, key, pointer.Value);
            TryIndexProperties(data, pointer.Value);

            return pointer;
        }

        public Result<T> Update<T>(string key, T newValue)
        {
            var findResult = FindValuePointerByKey<T>(key);
            if (!findResult.HasValue)
                return Result.Fail<T>(findResult.ErrorCode.Value, findResult.ErrorMsg);

            // modify md key
            var md = MdAccess.Locate(findResult.Value.XORAddress);
            var setResult = md.Set(key, new Value
            {
                Payload = newValue.Json(),
                ValueType = typeof(T).Name
            });
            if (setResult.HasValue)
                return Result.OK(newValue);
            else
                return Result.Fail<T>(setResult.ErrorCode.Value, setResult.ErrorMsg);

            // TODO: re-index
        }

        public Result<Pointer> Delete<T>(string key) // What to return? Result<(Pointer, Value)>, Result<Pointer>, Result<Value>, Result<T>
        {
            var findResult = FindValuePointerByKey<T>(key);
            if (!findResult.HasValue)
                return findResult;

            var md = MdAccess.Locate(findResult.Value.XORAddress);
            var deleteResult = md.Delete(key);
            return deleteResult;

            // TODO: re-index
        }

        Result<Pointer> FindValuePointerByKey<T>(string key)
        {
            var type = typeof(T).Name;
            var indexKey = $"{type}/{key}";
            var (data, errors) = _indexer.GetAllPointersWithValues(indexKey);
            var list = data.ToList();
            if (list.Count == 0)
                return new KeyNotFound<Pointer>(string.Join(',', errors));
            if (list.Count > 1)
                return new MultipleResults<Pointer>($"Expected 1 result, found: {list.Count}.");
            return Result.OK(list.Single().Item1);
        }

        // 1. Index on the type and key
        void IndexOnKey(string type, string key, Pointer valuePointer)
        {
            var indexKey = $"{type}/{key}";
            _indexer.Index(indexKey, valuePointer);
        }

        // 2. Find any other indices
        void TryIndexProperties(object data, Pointer valuePointer)
        {
            _indexer.TryIndex(data, valuePointer);
        }

        void AddStore<T>()
        {
            var type = typeof(T).Name;
            AddStore(type);
        }

        protected void AddStore(string type)
        {
            if (_dataTreeAddresses.ContainsKey(type))
                return;

            void onHeadChange(byte[] newXOR) => UpdateTypeStores(type, newXOR);
            
            var dataTree = DataTreeFactory.Create(onHeadChange);
            _dataTreeCache[type] = dataTree;
            _dataTreeAddresses[type] = dataTree.XORAddress;
            _typeInfo.Add(type, dataTree.XORAddress);
        }

        protected void LoadStore(string type)
        {
            if (!_dataTreeAddresses.ContainsKey(type))
                throw new InvalidOperationException($"Store does not exist! {type}");

            void onHeadChange(byte[] newXOR) => UpdateTypeStores(type, newXOR);
            var head = MdAccess.Locate(_dataTreeAddresses[type]);
            var dataTree = new DataTree(head, onHeadChange);
            _dataTreeCache[type] = dataTree;
        }

        void UpdateTypeStores(string type, byte[] XORAddress)
        {
            _typeInfo.Update(type, XORAddress);
            _dataTreeAddresses[type] = XORAddress;
        }
    }
}
