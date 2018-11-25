using System.Text;
using System.Threading.Tasks;

namespace SAFE.DataAccess.FileSystems
{
    public class DirectoryFactory
    {
        public static async Task<Result<Directory>> GetOrAddAsync(string directoryPath)
        {
            var indexer = await IndexerFactory.GetOrAddAsync(directoryPath);
            if (!indexer.HasValue)
                return Result.Fail<Directory>(indexer.ErrorCode.Value, indexer.ErrorMsg);

            var dirLocation = new MdLocator(Encoding.UTF8.GetBytes(directoryPath), DataProtocol.DEFAULT_PROTOCOL);
            var dirMd = await MdAccess.LocateAsync(dirLocation).ConfigureAwait(false);
            if (!dirMd.HasValue)
                return Result.Fail<Directory>(dirMd.ErrorCode.Value, dirMd.ErrorMsg);
            var dirHead = new MdHead(dirMd.Value, directoryPath);
            var dirResult = await Directory.GetOrAddAsync(dirHead, indexer.Value);
            return dirResult;
        }

        public static async Task<Result<Directory>> GetOrAddAsync(string directoryPath, MdLocator dirLocator)
        {
            var indexer = await IndexerFactory.GetOrAddAsync(directoryPath);
            if (!indexer.HasValue)
                return Result.Fail<Directory>(indexer.ErrorCode.Value, indexer.ErrorMsg);

            var dirMd = await MdAccess.LocateAsync(dirLocator).ConfigureAwait(false);
            if (!dirMd.HasValue)
                return Result.Fail<Directory>(dirMd.ErrorCode.Value, dirMd.ErrorMsg);
            var dirHead = new MdHead(dirMd.Value, directoryPath);
            var dirResult = await Directory.GetOrAddAsync(dirHead, indexer.Value);
            return dirResult;
        }
    }

    public class IndexerFactory
    {
        public static async Task<Result<IIndexer>> GetOrAddAsync(string directoryPath)
        {
            var indexLocation = new MdLocator(Encoding.UTF8.GetBytes($"{directoryPath}_indexer"), DataProtocol.DEFAULT_PROTOCOL);
            var indexMd = await MdAccess.LocateAsync(indexLocation).ConfigureAwait(false);
            if (!indexMd.HasValue)
                return Result.Fail<IIndexer>(indexMd.ErrorCode.Value, indexMd.ErrorMsg);
            var indexHead = new MdHead(indexMd.Value, directoryPath);
            var indexer = await Indexer.GetOrAddAsync(indexHead);
            return Result.OK((IIndexer)indexer);
        }
    }
}
