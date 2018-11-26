using System;
using System.IO;

namespace SAFE.DataAccess.FileSystems
{
    public class MdFileStream : Stream
    {
        private readonly MdFileInfo _file;

        public byte[] Content { get; private set; }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length
        {
            get { return _file.Content.Length; }
        }

        public override long Position { get; set; }

        public MdFileStream(MdFileInfo file)
        {
            _file = file;
        }

        public override void Flush()
        {
            _file.WriteContent(Content);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (origin == SeekOrigin.Begin)
                return Position = offset;
            if (origin == SeekOrigin.Current)
                return Position += offset;
            return Position = Length - offset;
        }

        public override void SetLength(long value)
        {
            Content = _file.ReadContent();
            int newLength = (int)value;
            byte[] newContent = new byte[newLength];
            Buffer.BlockCopy(Content, 0, newContent, 0, Math.Min(newLength, (int)Length));
            Content = newContent;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            Content = _file.ReadContent();
            int mincount = Math.Min(count, Math.Abs((int)(Length - Position)));
            Buffer.BlockCopy(Content, (int)Position, buffer, offset, mincount);
            Position += mincount;
            return mincount;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (Length - Position < count)
                SetLength(Position + count);
            Buffer.BlockCopy(buffer, offset, Content, (int)Position, count);
            Position += count;
            //_file.WriteContent(Content);
        }
    }
}
