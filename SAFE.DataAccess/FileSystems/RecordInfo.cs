
using System;

namespace SAFE.DataAccess.FileSystems
{
    public abstract class RecordInfo
    {
        protected readonly Func<string, Result<StoredValue>> _infoReader;
        protected readonly Func<string, StoredValue, Result<Pointer>> _infoWriter;

        public RecordInfo(string path, MdLocator locator, 
            Func<string, Result<StoredValue>> infoReader, Func<string, StoredValue, Result<Pointer>> infoWriter)
        {
            Path = path;
            Locator = locator;
            _infoReader = infoReader;
            _infoWriter = infoWriter;
        }

        public string Path { get; }
        public MdLocator Locator { get; }
    }

    public class DirectoryInfo : RecordInfo
    {
        public DirectoryInfo(string path, MdLocator locator, 
            Func<string, Result<StoredValue>> infoReader, Func<string, StoredValue, Result<Pointer>> infoWriter)
               : base(path, locator, infoReader, infoWriter)
        { }
    }

    public class MdFileInfo : RecordInfo
    {
        public MdFileInfo(string path, MdLocator locator,
            Func<string, Result<StoredValue>> infoReader, Func<string, StoredValue, Result<Pointer>> infoWriter)
            : base(path, locator, infoReader, infoWriter)
        { }

        public byte[] Content { get; private set; } = new byte[0];

        public byte[] ReadContent()
        {
            if (Content.Length == 0)
            {
                var contentResult = _infoReader($"{Path}/Content");

                if (!contentResult.HasValue)
                    Content = new byte[0];
                else
                    Content = contentResult.Value.Parse<byte[]>();
            }
            return Content;
        }

        public void WriteContent(byte[] value)
        {
            var data = new StoredValue(value);
            var res = _infoWriter($"{Path}/Content", data);
            if (res.HasValue)
                Content = value;
            else
                throw new Exception(res.ErrorMsg);
        }
    }
}
