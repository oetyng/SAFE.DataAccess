using System.Text;

namespace SAFE.DataAccess.Factories
{
    public class TypeStoreInfoFactory
    {
        public const string TYPE_STORE_HEAD_KEY = "TYPE_STORE_HEAD";

        public static TypeStoreInfo GetOrAddTypeStore(IMd dbInfoMd, string dbId)
        {
            IMd typeStoreHead;
            var typeStoreResult = dbInfoMd.GetValue(TYPE_STORE_HEAD_KEY);
            if (!typeStoreResult.HasValue)
            {
                typeStoreHead = MdAccess.Locate(Encoding.UTF8.GetBytes($"{TYPE_STORE_HEAD_KEY}_{dbId}"));
                dbInfoMd.Add(TYPE_STORE_HEAD_KEY, new Value(typeStoreHead.XORAddress));
            }
            else
            {
                var typeStoreHeadXOR = typeStoreResult.Value.Payload.Parse<byte[]>();
                typeStoreHead = MdAccess.Locate(typeStoreHeadXOR);
            }

            void onHeadChange(byte[] newXOR) => dbInfoMd.Set(TYPE_STORE_HEAD_KEY, new Value(newXOR));

            var dataTree = new DataTree(typeStoreHead, onHeadChange);

            return new TypeStoreInfo(dataTree);
        }
    }
}
