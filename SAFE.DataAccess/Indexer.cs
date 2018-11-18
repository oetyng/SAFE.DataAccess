using SAFE.DataAccess.Factories;
using System.Collections.Generic;
using System.Linq;

namespace SAFE.DataAccess
{
    public class Indexer : Database, IIndexer
    {
        Dictionary<string, string[]> _paths = new Dictionary<string, string[]>();

        Indexer(IMd info, TypeStoreInfo typeStore)
            : base(info, typeStore, new InactiveIndexer())
        { }

        public static Indexer Create(string dbid)
        {
            var xor = System.Text.Encoding.UTF8.GetBytes($"{dbid}_indexer");
            var md = MdAccess.Locate(xor);
            var typeStoreInfo = TypeStoreInfoFactory.GetOrAddTypeStore(md, dbid);
            var indexer = new Indexer(md, typeStoreInfo);

            var typeStores = typeStoreInfo.GetAll();
            indexer._dataTreeAddresses = typeStores
                .ToDictionary(c => c.Item1, c => c.Item2);

            indexer._paths = indexer._dataTreeAddresses
                .Where(c => c.Key.Count(t => t == '/') == 2) // ayy.. must be better than this
                .ToDictionary(c => c.Key, c => c.Key.Split('/')[1].Split('.'));

            return indexer;
        }

        public bool ExistsIndex(string indexKey)
        {
            return _dataTreeAddresses.ContainsKey(indexKey);
        }

        public void CreateIndex<T>(string[] propertyPath, IEnumerable<(Pointer, Value)> pointerValues)
        {
            _paths = pointerValues
                .ToDictionary(data => 
                    TryIndexWithPath(propertyPath, data.Item2.Payload.Parse(), data.Item1),
                    c => propertyPath);
        }

        public void TryIndex(object topLevelObject, Pointer valuePointer)
        {
            foreach (var path in _paths.Values)
                TryIndexWithPath(path, topLevelObject, valuePointer);
        }

        public void Index(string indexKey, Pointer valuePointer)
        {
            if (!_dataTreeAddresses.ContainsKey(indexKey))
                AddStore(indexKey);
            if (!_dataTreeCache.ContainsKey(indexKey))
                LoadStore(indexKey);

            var value = new Value
            {
                Payload = valuePointer.Json(),
                ValueType = valuePointer.GetType().Name
            };
            var store = _dataTreeCache[indexKey];
            var pointer = _dataTreeCache[indexKey].Add(valuePointer.MdKey, value);

            if (!pointer.HasValue)
                throw new System.Exception(pointer.ErrorMsg);
        }

        // Scan. When db is acting as index db, this is 
        // what we want to do; i.e. fetch all that there is for the key.
        public (IEnumerable<T> data, IEnumerable<string> errors) GetAllValues<T>(string indexKey)
        {
            if (!_dataTreeCache.ContainsKey(indexKey))
                LoadStore(indexKey);

            var pointers = _dataTreeCache[indexKey]
                .GetAllValues() // in an IndexDb, leaf values are stored Pointers,
                .Select(c => c.Payload.Parse<Pointer>()); // as opposed to in ordinary Database, where leaf values are of type Value

            var results = pointers
                .Select(c => MdAccess.Locate(c.XORAddress).GetValue(c.MdKey));

            var data = results.Where(c => c.HasValue)
                .Select(c => c.Value)
                .Select(c => c.Payload.Parse<T>());
            var errors = results.Where(c => !c.HasValue)
                .Select(c => c.ErrorMsg);

            return (data, errors);
        }

        // Scan. When db is acting as index db, this is 
        // what we want to do; i.e. fetch all that there is for the key.
        public (IEnumerable<(Pointer, Value)> data, IEnumerable<string> errors) GetAllPointersWithValues(string indexKey)
        {
            if (!_dataTreeCache.ContainsKey(indexKey))
                LoadStore(indexKey);

            var pointers = _dataTreeCache[indexKey]
                .GetAllValues()
                .Select(c => c.Payload.Parse<Pointer>());

            var results = pointers
                .Select(c => MdAccess.Locate(c.XORAddress).GetPointerAndValue(c.MdKey));

            var data = results.Where(c => c.HasValue)
                .Select(c => c.Value);
            var errors = results.Where(c => !c.HasValue)
                .Select(c => c.ErrorMsg);

            return (data, errors);
        }

        string TryIndexWithPath(string[] propertyPath, object topLevelObject, Pointer valuePointer)
        {
            object currentObj = topLevelObject;
            foreach (var prop in propertyPath)
            {
                var propInfo = currentObj.GetType().GetProperty(prop);
                currentObj = propInfo.GetValue(currentObj);
            }

            var type = topLevelObject.GetType().Name;
            string path = string.Join('.', propertyPath);
            var key = currentObj.ToString();
            var indexKey = $"{type}/{path}/{key}";
            Index(indexKey, valuePointer);
            return indexKey;
        }
    }

    class InactiveIndexer : IIndexer
    {
        public void CreateIndex<T>(string[] propertyPath, IEnumerable<(Pointer, Value)> pointerValues)
        {
            // No op
        }

        public (IEnumerable<T>, IEnumerable<string>) GetAllValues<T>(string indexKey)
        {
            return (new List<T>(), new List<string>());
        }

        public (IEnumerable<(Pointer, Value)> data, IEnumerable<string> errors) GetAllPointersWithValues(string indexKey)
        {
            return (new List<(Pointer, Value)>(), new List<string>());
        }

        public void Index(string indexKey, Pointer valuePointer)
        {
            // No op
        }

        public void TryIndex(object topLevelObject, Pointer valuePointer)
        {
            // No op
        }
    }
}
