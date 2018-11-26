using SAFE.DataAccess.FileSystems;
using SAFE.DataAccess.Network;
using System.Threading.Tasks;

namespace SAFE.DataAccess.Client
{
    public class FileSystemFactory
    {
        public static Result<FileSystem> CreateInMemory(string root = "/")
        {
            MdAccess.UseInMemoryDb();
            return FileSystem.GetOrAdd(root);
        }

        public static async Task<Result<FileSystem>> MockSAFENetwork(string root = "/")
        {
            var appId = TestAppCreation.GetRandomString(10);
            var session = await TestAppCreation.CreateTestApp(appId);
            return await CreateForSAFEApp(session.Value, appId, root);
        }

        public static async Task<Result<FileSystem>> CreateForSAFEApp(SafeApp.Session session, string appId, string root)
        {
            var manager = new MdHeadManager(session, appId, DataProtocol.DEFAULT_PROTOCOL);
            await manager.InitializeManager();

            MdAccess.SetCreator((level) => manager.CreateNewMdOps(level, DataProtocol.DEFAULT_PROTOCOL));
            MdAccess.SetLocator(manager.LocateMdOps);

            var dbResult = FileSystem.GetOrAdd(root);
            return dbResult;
        }
    }
}
