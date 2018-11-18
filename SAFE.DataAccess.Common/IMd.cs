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
        byte[] XORAddress { get; }

        Task<Result<Pointer>> AddAsync(Pointer pointer);
        Task<Result<Pointer>> AddAsync(string key, Value value);
        Task<Result<Pointer>> DeleteAsync(string key);
        Task<IEnumerable<(Pointer, Value)>> GetAllPointerValuesAsync();
        Task<Result<(Pointer, Value)>> GetPointerAndValueAsync(string key);
        Task<Result<Value>> GetValueAsync(string key);
        Task<IEnumerable<Value>> GetAllValuesAsync();
        Task<Result<Pointer>> SetAsync(string key, Value value, long expectedVersion = -1);
    }
}