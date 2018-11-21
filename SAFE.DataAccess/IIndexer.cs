using System.Collections.Generic;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    public interface IIndexer
    {
        Task CreateIndexAsync<T>(string[] propertyPath, IEnumerable<(Pointer, StoredValue)> pointerValues);
        Task TryIndexAsync(object topLevelObject, Pointer valuePointer);
        Task IndexAsync(string indexKey, Pointer valuePointer);
        Task<(IEnumerable<T> data, IEnumerable<string> errors)> GetAllValuesAsync<T>(string indexKey);
        Task<(IEnumerable<(Pointer, StoredValue)> data, IEnumerable<string> errors)> GetAllPointersWithValuesAsync(string indexKey);
    }
}
