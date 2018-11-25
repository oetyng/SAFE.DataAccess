using SAFE.DataAccess.Factories;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess.FileSystems
{
    public class FileSystem : IFileSystem
    {
        Directory _root;

        private FileSystem(Directory root)
        {
            _root = root;
        }

        public static Result<FileSystem> GetOrAdd(string root)
        {
            var path = FileSystemPath.Parse(root);
            if (!path.IsRoot)
                return new InvalidOperation<FileSystem>("Must be root path");

            var dir = DirectoryFactory.GetOrAddAsync(root).GetAwaiter().GetResult();
            if (!dir.HasValue)
                return Result.Fail<FileSystem>(dir.ErrorCode.Value, dir.ErrorMsg);
            return Result.OK(new FileSystem(dir.Value));
        }

        public void CreateDirectory(FileSystemPath path)
        {
            var dir = GetParentDir(path);
            var createResult = dir.CreateSubDirectory(path).GetAwaiter().GetResult();
            if (!createResult.HasValue)
                throw new Exception(createResult.ErrorMsg);
        }

        public Stream CreateFile(FileSystemPath path)
        {
            var dir = GetParentDir(path);
            var fileResult = dir.CreateFile(path).GetAwaiter().GetResult();
            if (fileResult.HasValue)
                return fileResult.Value;
            else
                throw new Exception(fileResult.ErrorMsg);
        }

        public void Delete(FileSystemPath path)
        {
            var dir = GetParentDir(path);
            if (path.IsFile)
            {
                var delRes = dir.DeleteFile(path).GetAwaiter().GetResult();
                if (!delRes.HasValue)
                    throw new Exception(delRes.ErrorMsg);
            }
            else if (path.IsDirectory)
            {
                var delRes = dir.DeleteDirectory(path).GetAwaiter().GetResult();
                if (!delRes.HasValue)
                    throw new Exception(delRes.ErrorMsg);
            }
        }

        public void Dispose()
        {
            // some op
        }

        public bool Exists(FileSystemPath path)
        {
            try
            {
                var dir = GetParentDir(path);
                var result = dir.ExistsAsync(path).GetAwaiter().GetResult();
                return result.HasValue && result.Value;
            }
            catch(DirectoryNotFoundException ex)
            {
                return false;
            }

        }

        public ICollection<FileSystemPath> GetEntities(FileSystemPath path)
        {
            throw new NotImplementedException();
        }

        public Stream OpenFile(FileSystemPath path, FileAccess access)
        {
            var dir = GetParentDir(path);
            var fileResult = dir.FindFileAsync(path).GetAwaiter().GetResult();
            if (fileResult.HasValue)
                return fileResult.Value;
            else
                throw new Exception(fileResult.ErrorMsg);
        }


        Directory GetParentDir(FileSystemPath path)
        {
            var dirResult = _root.GetParentDir(path).GetAwaiter().GetResult();
            if (!dirResult.HasValue)
                throw new DirectoryNotFoundException(dirResult.ErrorMsg);
            return dirResult.Value;
        }
    }
}
