using System;
using System.Buffers;
using System.IO;

namespace Sir.KeyValue
{
    /// <summary>
    /// Find the address of a value by supplying a value ID.
    /// </summary>
    public class ValueIndexReader : IDisposable
    {
        private readonly Stream _stream;
        private const int BlockSize = sizeof(long) + sizeof(int) + sizeof(byte);

        public ValueIndexReader(Stream stream)
        {
            _stream = stream;
        }

        public void Dispose()
        {
            if (_stream != null)
                _stream.Dispose();
        }

        public (long offset, int len, byte dataType) Get(long id)
        {
            var offset = id * BlockSize;

            _stream.Seek(offset, SeekOrigin.Begin);

            Span<byte> buf = ArrayPool<byte>.Shared.Rent(BlockSize);
             _stream.Read(buf);

            return (BitConverter.ToInt64(buf.Slice(0)), BitConverter.ToInt32(buf.Slice(sizeof(long))), buf[BlockSize - 1]);
        }
    }
}
