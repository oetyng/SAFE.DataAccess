using System.Text;
using System.Threading.Tasks;

namespace SAFE.DataAccess.Factories
{
    public class TypeStoreInfoFactory
    {
        public const string TYPE_STORE_HEAD_KEY = "TYPE_STORE_HEAD";

        public static async Task<TypeStoreInfo> GetOrAddTypeStoreAsync(IMd dbInfoMd, string dbId)
        {
            IMd typeStoreHead;
            var typeStoreResult = await dbInfoMd.GetValueAsync(TYPE_STORE_HEAD_KEY).ConfigureAwait(false);
            if (!typeStoreResult.HasValue)
            {
                typeStoreHead = await MdAccess.LocateAsync(Encoding.UTF8.GetBytes($"{TYPE_STORE_HEAD_KEY}_{dbId}"))
                    .ConfigureAwait(false);
                await dbInfoMd.AddAsync(TYPE_STORE_HEAD_KEY, new Value(typeStoreHead.XORAddress))
                    .ConfigureAwait(false);
            }
            else
            {
                var typeStoreHeadXOR = typeStoreResult.Value.Payload.Parse<byte[]>();
                typeStoreHead = await MdAccess.LocateAsync(typeStoreHeadXOR)
                    .ConfigureAwait(false);
            }

            Task onHeadChange(byte[] newXOR) => dbInfoMd.SetAsync(TYPE_STORE_HEAD_KEY, new Value(newXOR));

            var dataTree = new DataTree(typeStoreHead, onHeadChange);

            return new TypeStoreInfo(dataTree);
        }
    }
}
