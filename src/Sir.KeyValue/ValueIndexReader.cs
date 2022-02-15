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

            var buf = ArrayPool<byte>.Shared.Rent(BlockSize);

             _stream.Read(buf);

            var addr = (BitConverter.ToInt64(buf), BitConverter.ToInt32(buf, sizeof(long)), buf[BlockSize - 1]);

            ArrayPool<byte>.Shared.Return(buf);

            return addr;
        }
    }
}
