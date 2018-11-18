using System.Collections.Generic;
using System.Linq;

namespace SAFE.DataAccess
{
    public class TypeStoreInfo
    {
        DataTree _dataTree;

        public TypeStoreInfo(DataTree dataTree)
        {
            _dataTree = dataTree;
        }

        public void Add(string type, byte[] address)
        {
            var value = new Value
            {
                Payload = address.Json(),
                ValueType = typeof(byte[]).GetType().Name
            };
            _dataTree.Add(type, value);
        }

        public Result<Pointer> Update(string type, byte[] address)
        {
            var (pointer, value) = _dataTree.GetAllPointerValues()
                .Single(c => c.Item1.MdKey == type);
            var md = MdAccess.Locate(pointer.XORAddress);
            value.Payload = address.Json();
            return md.Set(type, value);
        }

        public IEnumerable<(string, byte[])> GetAll()
        {
            var some = _dataTree.GetAllPointerValues()
                .Select(c => (c.Item1.MdKey, c.Item2.Payload.Parse<byte[]>()));
            return some;
        }
    }
}
