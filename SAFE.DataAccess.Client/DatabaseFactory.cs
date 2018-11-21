using SAFE.DataAccess.Network;
using System.Threading.Tasks;

namespace SAFE.DataAccess.Client
{
    public class DatabaseFactory
    {
        public static async Task<Result<Database>> CreateForApp(SafeApp.Session session, string appId, string databaseId)
        {
            var manager = new MdHeadManager(session, appId, DataProtocol.DEFAULT_PROTOCOL);
            await manager.InitializeManager();

            MdAccess.SetCreator((level) => manager.CreateNewMdOps(level, DataProtocol.DEFAULT_PROTOCOL));
            MdAccess.SetLocator(manager.LocateMdOps);

            var indexerDbId = $"{databaseId}_indexer";
            var indexerMdHead = await manager.GetOrAddHeadAsync(indexerDbId);
            var indexer = await Indexer.GetOrAddAsync(indexerMdHead);

            var databaseMdHead = await manager.GetOrAddHeadAsync(databaseId);
            var dbResult = await Database.GetOrAddAsync(databaseMdHead, indexer);
            return dbResult;
        }
    }
}
