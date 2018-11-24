using System.Threading.Tasks;

namespace SAFE.DataAccess.Client
{
    public class ClientFactory
    {
        public static IClient GetInMemoryClient()
        {
            return new InMemClient();
        }

        public static async Task<IClient> GetMockNetworkClient()
        {
            var appId = TestAppCreation.GetRandomString(10);
            var session = await TestAppCreation.CreateTestApp(appId);
            return new SAFEClient(session.Value, appId);
        }

        // GetAlpha-2Client
    }

    public interface IClient
    {
        Task<Result<Database>> GetOrAddDataBaseAsync(string dbName);
    }

    public class InMemClient : IClient
    {
        public InMemClient()
        {
            MdAccess.UseInMemoryDb();
        }

        public async Task<Result<Database>> GetOrAddDataBaseAsync(string dbName)
        {
            var indexLocation = new MdLocator(System.Text.Encoding.UTF8.GetBytes($"{dbName}_indexer"), DataProtocol.DEFAULT_PROTOCOL);
            var indexMd = await MdAccess.LocateAsync(indexLocation).ConfigureAwait(false);
            if (!indexMd.HasValue)
                return Result.Fail<Database>(indexMd.ErrorCode.Value, indexMd.ErrorMsg);
            var indexHead = new MdHead(indexMd.Value, dbName);
            var indexer = await Indexer.GetOrAddAsync(indexHead);

            var dbLocation = new MdLocator(System.Text.Encoding.UTF8.GetBytes(dbName), DataProtocol.DEFAULT_PROTOCOL);
            var dbMd = await MdAccess.LocateAsync(dbLocation).ConfigureAwait(false);
            if (!dbMd.HasValue)
                return Result.Fail<Database>(dbMd.ErrorCode.Value, dbMd.ErrorMsg);
            var dbHead = new MdHead(dbMd.Value, dbName);
            var dbResult = await Database.GetOrAddAsync(dbHead, indexer);
            return dbResult;
        }
    }

    public class SAFEClient : IClient
    {
        readonly SafeApp.Session _session;
        readonly string _appId;

        public SAFEClient(SafeApp.Session session, string appId)
        {
            _session = session;
            _appId = appId;
        }

        public async Task<Result<Database>> GetOrAddDataBaseAsync(string dbName)
        {
            var dbResult = await DatabaseFactory.CreateForApp(_session, _appId, dbName);
            return dbResult;
        }
    }
}