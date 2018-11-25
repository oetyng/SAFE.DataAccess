
using System;

namespace SAFE.DataAccess.FileSystems
{
    public abstract class RecordInfo
    {
        [NonSerialized]
        protected IMd _md;

        public RecordInfo(IMd md)
        {
            _md = md;
        }

        public MdLocator Locator => _md.MdLocator;
    }

    public class DirectoryInfo : RecordInfo
    {
        public DirectoryInfo(IMd md)
               : base(md)
        { }
    }

    public class MdFileInfo : RecordInfo
    {
        byte[] _content;

        public MdFileInfo(IMd md)
            : base(md)
        { }

        public byte[] Content
        {
            get
            {
                if (_content == null)
                {
                    var contentResult = _md.GetValueAsync("Content").GetAwaiter().GetResult();

                    if (!contentResult.HasValue)
                        _content = new byte[0];
                    else
                        _content = contentResult.Value.Parse<byte[]>();
                }
                return _content;
            }
            set
            {
                var data = new StoredValue(value);
                var res = _md.SetAsync("Content", data).GetAwaiter().GetResult();
                if (res.HasValue)
                    _content = value;
                else
                    throw new Exception(res.ErrorMsg);
            }
        }

    }
}
