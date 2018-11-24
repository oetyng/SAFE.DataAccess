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
                typeStoreHead = await MdAccess.CreateAsync(0)
                    .ConfigureAwait(false);
                await dbInfoMd.AddAsync(TYPE_STORE_HEAD_KEY, new StoredValue(typeStoreHead.MdLocator))
                    .ConfigureAwait(false);
            }
            else
            {
                var typeStoreHeadLocation = typeStoreResult.Value.Payload.Parse<MdLocator>();
                typeStoreHead = (await MdAccess.LocateAsync(typeStoreHeadLocation)
                    .ConfigureAwait(false)).Value;
            }

            Task onHeadChange(MdLocator newLocation) => dbInfoMd.SetAsync(TYPE_STORE_HEAD_KEY, new StoredValue(newLocation));

            var dataTree = new DataTree(typeStoreHead, onHeadChange);

            return new TypeStoreInfo(dataTree);
        }
    }
}
