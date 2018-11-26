using SAFE.DataAccess.Factories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess.FileSystems
{
    public class Directory
    {
        IMd _info;
        TypeStoreInfo _typeInfo;
        protected Dictionary<string, MdLocator> _dataTreeAddresses = new Dictionary<string, MdLocator>();
        protected Dictionary<string, DataTree> _dataTreeCache = new Dictionary<string, DataTree>();

        IIndexer _indexer;

        FileSystemPath _path;

        const string FILE_INFO_KEY = nameof(MdFileInfo);
        const string DIR_INFO_KEY = nameof(DirectoryInfo);

        protected Directory(IMd info, TypeStoreInfo typeInfo, IIndexer indexer)
        {
            _info = info;
            _typeInfo = typeInfo;
            _indexer = indexer;
        }

        public static async Task<Result<Directory>> GetOrAddAsync(MdHead mdHead, IIndexer indexer)
        {
            var typeInfo = await TypeStoreInfoFactory.GetOrAddTypeStoreAsync(mdHead.Md, mdHead.Id).ConfigureAwait(false);
            var db = new Directory(mdHead.Md, typeInfo, indexer);

            var typeStores = await typeInfo.GetAllAsync().ConfigureAwait(false);
            db._dataTreeAddresses = typeStores
                .ToDictionary(c => c.Item1, c => c.Item2);

            return Result.OK(db);
        }

        public async Task<Result<Directory>> GetParentDir(FileSystemPath path)
        {
            if (_path.IsParentOf(path))
                return Result.OK(this);

            var parent = path.ParentPath;
            while (!_path.IsParentOf(parent))
            {
                if (parent.IsRoot)
                    return new KeyNotFound<Directory>("Path does not belong to this tree.");
                parent = parent.ParentPath;
            }

            var dirResult = await DirectoryFactory.GetOrAddAsync(path.Path);
            if (!dirResult.HasValue)
                return dirResult;

            return await dirResult.Value.GetParentDir(path);
        }

        public async Task<Result<bool>> ExistsAsync(FileSystemPath path)
        {
            bool exists = false;

            if (path.IsFile)
                exists = (await FindFileAsync(path)).HasValue;
            else if (path.IsDirectory)
                exists = (await FindDirectoryAsync(path)).HasValue;
            else
                return new KeyNotFound<bool>("Unknown path type");

            return Result.OK(exists);
        }

        public async Task<Result<MdFileStream>> FindFileAsync(FileSystemPath path)
        {
            var info = await FindFileInfoAsync(path);
            if (!info.HasValue)
                return new KeyNotFound<MdFileStream>(info.ErrorMsg);
            return Result.OK(new MdFileStream(info.Value));
        }

        public async Task<Result<Directory>> FindDirectoryAsync(FileSystemPath path)
        {
            var info = await FindDirectoryInfoAsync(path);
            if (!info.HasValue)
                return new KeyNotFound<Directory>(info.ErrorMsg);

            var dirResult = await DirectoryFactory.GetOrAddAsync(path.Path, info.Value.Locator);
            return dirResult;
        }

        public async Task<Result<MdFileStream>> CreateFile(FileSystemPath path)
        {
            if (!_path.IsParentOf(path))
                return new InvalidOperation<MdFileStream>("Incorrect path");
            if (!path.IsFile)
                return new InvalidOperation<MdFileStream>("Path is not a file");

            var fileResult = await FindFileAsync(path);
            if (fileResult.HasValue)
                return new ValueAlreadyExists<MdFileStream>(path.Path);

            await AddOrLoad(FILE_INFO_KEY);

            var md = await MdAccess.CreateAsync(0);
            var info = SetupFileInfo(path, md);
            var value = new StoredValue(info);
            var pointer = await _dataTreeCache[FILE_INFO_KEY].AddAsync(path.Path, value);
            if (!pointer.HasValue)
                return Result.Fail<MdFileStream>(pointer.ErrorCode.Value, pointer.ErrorMsg);

            await _indexer.IndexAsync(path.Path, pointer.Value);
            //ScheduleIndexing(path, pointer.Value);

            return Result.OK(new MdFileStream(info));
        }

        public async Task<Result<Directory>> CreateSubDirectory(FileSystemPath path)
        {
            // validate that parent path is 
            if (!_path.IsParentOf(path))
                return new InvalidOperation<Directory>("Incorrect path");
            if (!path.IsDirectory)
                return new InvalidOperation<Directory>("Path is not a directory");

            var directoryResult = await FindDirectoryAsync(path);
            if (directoryResult.HasValue)
                return new ValueAlreadyExists<Directory>(path.Path);

            var res = await DirectoryFactory.GetOrAddAsync(path.Path);
            if (!res.HasValue)
                return res;

            await AddOrLoad(DIR_INFO_KEY);

            var info = SetupDirectoryInfo(path, res.Value._info);

            var value = new StoredValue(info.Locator);
            var pointer = await _dataTreeCache[DIR_INFO_KEY].AddAsync(path.Path, value);
            if (!pointer.HasValue)
                return Result.Fail<Directory>(pointer.ErrorCode.Value, pointer.ErrorMsg);

            await _indexer.IndexAsync(path.Path, pointer.Value);
            //ScheduleIndexing(path, pointer.Value);

            return res;
        }

        public async Task<Result<MdFileInfo>> DeleteFile(FileSystemPath path)
        {
            var findResult = await FindFileInfoAsync(path);
            if (!findResult.HasValue)
                return Result.Fail<MdFileInfo>(findResult.ErrorCode.Value, findResult.ErrorMsg);

            var mdResult = await MdAccess.LocateAsync(findResult.Value.Locator).ConfigureAwait(false);
            if (!mdResult.HasValue)
                return Result.Fail<MdFileInfo>(mdResult.ErrorCode.Value, mdResult.ErrorMsg);

            var deleteResult = await mdResult.Value.DeleteAsync(path.Path).ConfigureAwait(false);
            if (!deleteResult.HasValue)
                return Result.Fail<MdFileInfo>(deleteResult.ErrorCode.Value, deleteResult.ErrorMsg);

            return Result.OK(findResult.Value);

            // TODO: re-index
            //_indexer.Delete(key);
        }

        public async Task<Result<DirectoryInfo>> DeleteDirectory(FileSystemPath path)
        {
            var findResult = await FindDirectoryInfoAsync(path);
            if (!findResult.HasValue)
                return findResult;

            var mdResult = await MdAccess.LocateAsync(findResult.Value.Locator).ConfigureAwait(false);
            if (!mdResult.HasValue)
                return Result.Fail<DirectoryInfo>(mdResult.ErrorCode.Value, mdResult.ErrorMsg);

            var deleteResult = await mdResult.Value.DeleteAsync(path.Path).ConfigureAwait(false);
            if (!deleteResult.HasValue)
                return Result.Fail<DirectoryInfo>(deleteResult.ErrorCode.Value, deleteResult.ErrorMsg);

            return Result.OK(findResult.Value);

            // TODO: re-index
            //_indexer.Delete(key);
        }

        async Task<Result<MdFileInfo>> FindFileInfoAsync(FileSystemPath path)
        {
            if (!_path.IsParentOf(path))
                return new InvalidOperation<MdFileInfo>("Incorrect path");
            if (!path.IsFile)
                return new InvalidOperation<MdFileInfo>("Path is not a file");

            // a stored MdFileInfo is a reference (i.e. the locator) to the actual Md holding data
            var (data, errors) = await _indexer.GetAllValuesAsync<MdFileInfo>(path.Path).ConfigureAwait(false);
            var list = data.ToList();
            if (list.Count == 0)
                return new KeyNotFound<MdFileInfo>(string.Join(',', errors));
            if (list.Count > 1)
                return new MultipleResults<MdFileInfo>($"Expected 1 result, found: {list.Count}.");

            var md = await MdAccess.LocateAsync(list.Single().Locator); // finally locate the actual md, which our stored MdFileInfo uses to 
            return Result.OK(SetupFileInfo(path, md.Value)); // Get / Set actions will be injected into the encapsulating MdFileInfo, which acts directly upon its Md
        }

        async Task<Result<DirectoryInfo>> FindDirectoryInfoAsync(FileSystemPath path)
        {
            if (!_path.IsParentOf(path))
                return new InvalidOperation<DirectoryInfo>("Incorrect path");
            if (!path.IsDirectory)
                return new InvalidOperation<DirectoryInfo>("Path is not a directory");

            var (data, errors) = await _indexer.GetAllValuesAsync<DirectoryInfo>(path.Path).ConfigureAwait(false);
            var list = data.ToList();
            if (list.Count == 0)
                return new KeyNotFound<DirectoryInfo>(string.Join(',', errors));
            if (list.Count > 1)
                return new MultipleResults<DirectoryInfo>($"Expected 1 result, found: {list.Count}.");

            var md = await MdAccess.LocateAsync(list.Single().Locator);
            return Result.OK(SetupDirectoryInfo(path, md.Value));
        }

        // Get / Set actions will be injected into the encapsulating MdFileInfo, which acts directly upon its Md
        MdFileInfo SetupFileInfo(FileSystemPath path, IMd md)
        {
            var info = new MdFileInfo(path.Path, md.MdLocator,
                (key) => 
                    md.GetValueAsync(key).GetAwaiter().GetResult(),
                (key, val) => 
                {
                    var pointer = md.SetAsync(key, val).GetAwaiter().GetResult();
                    if (!pointer.HasValue)
                        return pointer;
                    return pointer;
                });
            return info;
        }

        DirectoryInfo SetupDirectoryInfo(FileSystemPath path, IMd md)
        {
            var info = new DirectoryInfo(path.Path, md.MdLocator,
                (key) => md.GetValueAsync(key).GetAwaiter().GetResult(),
                (key, val) =>
                {
                    var pointer = md.SetAsync(key, val).GetAwaiter().GetResult();
                    if (!pointer.HasValue)
                        return pointer;
                    return pointer;
                });
            return info;
        }

        async Task AddOrLoad(string store)
        {
            if (!_dataTreeAddresses.ContainsKey(store))
                await AddStoreAsync(store);
            if (!_dataTreeCache.ContainsKey(store))
                await LoadStoreAsync(store);
        }

        async Task AddStoreAsync(string type)
        {
            if (_dataTreeAddresses.ContainsKey(type))
                return;

            Task onHeadChange(MdLocator newLocation) => UpdateTypeStores(type, newLocation);

            var dataTree = await DataTreeFactory.CreateAsync(onHeadChange).ConfigureAwait(false);
            _dataTreeCache[type] = dataTree;
            _dataTreeAddresses[type] = dataTree.MdLocator;
            await _typeInfo.AddAsync(type, dataTree.MdLocator).ConfigureAwait(false);
        }

        async Task LoadStoreAsync(string type)
        {
            if (!_dataTreeAddresses.ContainsKey(type))
                throw new InvalidOperationException($"Store does not exist! {type}");
            // check if already loaded? or is this a refresh function also?

            Task onHeadChange(MdLocator newXOR) => UpdateTypeStores(type, newXOR);
            var headResult = await MdAccess.LocateAsync(_dataTreeAddresses[type]).ConfigureAwait(false);
            if (!headResult.HasValue)
                throw new Exception($"Error code: {headResult.ErrorCode.Value}. {headResult.ErrorMsg}");
            var dataTree = new DataTree(headResult.Value, onHeadChange);
            _dataTreeCache[type] = dataTree;
        }

        async Task UpdateTypeStores(string type, MdLocator location)
        {
            await _typeInfo.UpdateAsync(type, location).ConfigureAwait(false);
            _dataTreeAddresses[type] = location;
        }

        void ScheduleIndexing(FileSystemPath path, Pointer pointer)
        {
            Task.Factory.StartNew(() => _indexer.IndexAsync(path.Path, pointer));
        }
    }
}
