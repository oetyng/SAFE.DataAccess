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

        public async Task AddAsync(string type, byte[] address)
        {
            var value = new Value
            {
                Payload = address.Json(),
                ValueType = typeof(byte[]).GetType().Name
            };
            await _dataTree.AddAsync(type, value).ConfigureAwait(false);
        }

        public async Task<Result<Pointer>> UpdateAsync(string type, byte[] address)
        {
            var (pointer, value) = (await _dataTree.GetAllPointerValuesAsync().ConfigureAwait(false))
                .Single(c => c.Item1.MdKey == type);
            var md = await MdAccess.LocateAsync(pointer.XORAddress).ConfigureAwait(false);
            value.Payload = address.Json();
            return await md.SetAsync(type, value).ConfigureAwait(false);
        }

        public async Task<IEnumerable<(string, byte[])>> GetAllAsync()
        {
            var typeInfo = (await _dataTree.GetAllPointerValuesAsync().ConfigureAwait(false))
                .Select(c => (c.Item1.MdKey, c.Item2.Payload.Parse<byte[]>()));
            return typeInfo;
        }
    }
}
