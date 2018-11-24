using System.Collections.Generic;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    public interface IMd
    {
        int Count { get; }
        bool IsFull { get; }
        int Level { get; }
        MdType Type { get; }
        MdLocator MdLocator { get; }

        Task<Result<Pointer>> AddAsync(Pointer pointer);
        Task<Result<Pointer>> AddAsync(string key, StoredValue value);
        Task<Result<Pointer>> DeleteAsync(string key);
        Task<IEnumerable<(Pointer, StoredValue)>> GetAllPointerValuesAsync();
        Task<Result<(Pointer, StoredValue)>> GetPointerAndValueAsync(string key);
        Task<Result<StoredValue>> GetValueAsync(string key);
        Task<IEnumerable<StoredValue>> GetAllValuesAsync();
        Task<Result<Pointer>> SetAsync(string key, StoredValue value, long expectedVersion = -1);
    }
}