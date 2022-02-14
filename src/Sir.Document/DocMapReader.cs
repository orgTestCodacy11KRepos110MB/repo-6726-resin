using System;
using System.Buffers;
using System.IO;

namespace Sir.Documents
{
    /// <summary>
    /// Read document maps (key_id/val_id) from the document map stream.
    /// A document map is needed to re-contruct a complete document.
    /// </summary>
    public class DocMapReader : IDisposable
    {
        private readonly Stream _stream;

        public DocMapReader(Stream stream)
        {
            _stream = stream;
        }

        public void Dispose()
        {
            if (_stream != null)
                _stream.Dispose();
        }

        public (long keyId, long valId)[] Get(long offset, int length)
        {
            _stream.Seek(offset, SeekOrigin.Begin);

            Span<byte> buf = ArrayPool<byte>.Shared.Rent(length);
            int read = _stream.Read(buf);

            if (read != length)
            {
                throw new InvalidDataException();
            }

            const int blockSize = sizeof(long) + sizeof(long);
            var blockCount = length / blockSize;
            var docMapping = new (long, long)[blockCount];

            for (int i = 0; i < blockCount; i++)
            {
                var offs = i * blockSize;
                var key = BitConverter.ToInt64(buf.Slice(offs));
                var val = BitConverter.ToInt64(buf.Slice(offs + sizeof(long)));

                docMapping[i] = (key, val);
            }

            return docMapping;
        }
    }
}
