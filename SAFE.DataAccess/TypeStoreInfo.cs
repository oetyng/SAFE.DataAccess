using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    public class TypeStoreInfo
    {
        DataTree _dataTree;

        public TypeStoreInfo(DataTree dataTree)
        {
            _dataTree = dataTree;
        }

        public async Task AddAsync(string type, MdLocation location)
        {
            var value = new StoredValue(location);
            await _dataTree.AddAsync(type, value).ConfigureAwait(false);
        }

        public async Task<Result<Pointer>> UpdateAsync(string type, MdLocation location)
        {
            var (pointer, value) = (await _dataTree.GetAllPointerValuesAsync().ConfigureAwait(false))
                .Single(c => c.Item1.MdKey == type);
            var mdResult = await MdAccess.LocateAsync(pointer.MdLocation).ConfigureAwait(false);
            if (!mdResult.HasValue)
                return Result.Fail<Pointer>(mdResult.ErrorCode.Value, mdResult.ErrorMsg);
            value.Payload = location.Json();
            return await mdResult.Value.SetAsync(type, value).ConfigureAwait(false);
        }

        public async Task<IEnumerable<(string, MdLocation)>> GetAllAsync()
        {
            var typeInfo = (await _dataTree.GetAllPointerValuesAsync().ConfigureAwait(false))
                .Select(c => (c.Item1.MdKey, c.Item2.Payload.Parse<MdLocation>()));
            return typeInfo;
        }
    }
}
