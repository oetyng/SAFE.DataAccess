using SAFE.DataAccess.Factories;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    public class Indexer : Database, IIndexer
    {
        ConcurrentDictionary<string, string[]> _paths = new ConcurrentDictionary<string, string[]>();

        Indexer(IMd info, TypeStoreInfo typeStore)
            : base(info, typeStore, new InactiveIndexer())
        { }

        public static async Task<Indexer> CreateAsync(string dbid)
        {
            var xor = System.Text.Encoding.UTF8.GetBytes($"{dbid}_indexer");
            var md = await MdAccess.LocateAsync(xor).ConfigureAwait(false);
            var typeStoreInfo = await TypeStoreInfoFactory
                .GetOrAddTypeStoreAsync(md, dbid)
                .ConfigureAwait(false);
            var indexer = new Indexer(md, typeStoreInfo);

            var typeStores = await typeStoreInfo.GetAllAsync()
                .ConfigureAwait(false);
            indexer._dataTreeAddresses = typeStores
                .ToDictionary(c => c.Item1, c => c.Item2);

            indexer._paths = new ConcurrentDictionary<string, string[]>(indexer._dataTreeAddresses
                .Where(c => c.Key.Count(t => t == '/') == 2) // ayy.. must be better than this
                .ToDictionary(c => c.Key, c => c.Key.Split('/')[1].Split('.')));

            return indexer;
        }

        public bool ExistsIndex(string indexKey)
        {
            return _dataTreeAddresses.ContainsKey(indexKey);
        }

        public Task CreateIndexAsync<T>(string[] propertyPath, IEnumerable<(Pointer, Value)> pointerValues)
        {
            var tasks = pointerValues.Select(async data =>
            {
                var key = await TryIndexWithPath(
                    propertyPath,
                    data.Item2.Payload.Parse(),
                    data.Item1).ConfigureAwait(false);
                _paths[key] = propertyPath;
            });
            return Task.WhenAll(tasks);
        }

        public Task TryIndexAsync(object topLevelObject, Pointer valuePointer)
        {
            var tasks = _paths.Values.Select(path =>
                TryIndexWithPath(path, topLevelObject, valuePointer));

            return Task.WhenAll(tasks);
        }

        public async Task IndexAsync(string indexKey, Pointer valuePointer)
        {
            if (!_dataTreeAddresses.ContainsKey(indexKey))
                await AddStoreAsync(indexKey).ConfigureAwait(false);
            if (!_dataTreeCache.ContainsKey(indexKey))
                await LoadStoreAsync(indexKey).ConfigureAwait(false);

            var value = new Value
            {
                Payload = valuePointer.Json(),
                ValueType = valuePointer.GetType().Name
            };
            var store = _dataTreeCache[indexKey];
            var pointer = await _dataTreeCache[indexKey]
                .AddAsync(valuePointer.MdKey, value)
                .ConfigureAwait(false);

            if (!pointer.HasValue)
                throw new System.Exception(pointer.ErrorMsg);
        }

        // Scan. When db is acting as index db, this is 
        // what we want to do; i.e. fetch all that there is for the key.
        public async Task<(IEnumerable<T> data, IEnumerable<string> errors)> GetAllValuesAsync<T>(string indexKey)
        {
            if (!_dataTreeCache.ContainsKey(indexKey))
                await LoadStoreAsync(indexKey).ConfigureAwait(false);

            var pointers = (await _dataTreeCache[indexKey]
                .GetAllValuesAsync().ConfigureAwait(false)) // in an IndexDb, leaf values are stored Pointers,
                .Select(c => c.Payload.Parse<Pointer>()); // as opposed to in ordinary Database, where leaf values are of type Value

            var results = await Task.WhenAll(pointers
                .Select(async c =>
                {
                    var md = await MdAccess.LocateAsync(c.XORAddress).ConfigureAwait(false);
                    return await md.GetValueAsync(c.MdKey).ConfigureAwait(false);
                }));

            var data = results.Where(c => c.HasValue)
                .Select(c => c.Value)
                .Select(c => c.Payload.Parse<T>());
            var errors = results.Where(c => !c.HasValue)
                .Select(c => c.ErrorMsg);

            return (data, errors);
        }

        // Scan. When db is acting as index db, this is 
        // what we want to do; i.e. fetch all that there is for the key.
        public async Task<(IEnumerable<(Pointer, Value)> data, IEnumerable<string> errors)> GetAllPointersWithValuesAsync(string indexKey)
        {
            if (!_dataTreeCache.ContainsKey(indexKey))
                await LoadStoreAsync(indexKey).ConfigureAwait(false);

            var pointers = (await _dataTreeCache[indexKey]
                .GetAllValuesAsync().ConfigureAwait(false))
                .Select(c => c.Payload.Parse<Pointer>());


            var results = await Task.WhenAll(pointers
                .Select(async c =>
                {
                    var md = await MdAccess.LocateAsync(c.XORAddress).ConfigureAwait(false);
                    return await md.GetPointerAndValueAsync(c.MdKey).ConfigureAwait(false);
                }));

            var data = results.Where(c => c.HasValue)
                .Select(c => c.Value);
            var errors = results.Where(c => !c.HasValue)
                .Select(c => c.ErrorMsg);

            return (data, errors);
        }

        async Task<string> TryIndexWithPath(string[] propertyPath, object topLevelObject, Pointer valuePointer)
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
            await IndexAsync(indexKey, valuePointer).ConfigureAwait(false);
            return indexKey;
        }
    }

    class InactiveIndexer : IIndexer
    {
        public Task CreateIndexAsync<T>(string[] propertyPath, IEnumerable<(Pointer, Value)> pointerValues)
        {
            // No op
            return Task.FromResult(0);
        }

        public Task TryIndexAsync(object topLevelObject, Pointer valuePointer)
        {
            // No op
            return Task.FromResult(0);
        }

        public Task IndexAsync(string indexKey, Pointer valuePointer)
        {
            // No op
            return Task.FromResult(0);
        }

        public Task<(IEnumerable<T> data, IEnumerable<string> errors)> GetAllValuesAsync<T>(string indexKey)
        {
            // No op
            return Task.FromResult(((IEnumerable<T>)new List<T>(), (IEnumerable<string>)new List<string>()));
        }

        public Task<(IEnumerable<(Pointer, Value)> data, IEnumerable<string> errors)> GetAllPointersWithValuesAsync(string indexKey)
        {
            // No op
            return Task.FromResult(((IEnumerable<(Pointer, Value)>)new List<(Pointer, Value)>(), (IEnumerable<string>)new List<string>()));
        }
    }
}
