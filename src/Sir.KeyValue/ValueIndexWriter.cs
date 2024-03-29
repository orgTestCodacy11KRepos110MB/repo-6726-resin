﻿using System;
using System.IO;

namespace Sir.KeyValue
{
    /// <summary>
    /// Store the address of a value.
    /// </summary>
    public class ValueIndexWriter : IDisposable
    {
        private readonly Stream _stream;
        private static int _blockSize = sizeof(long) + sizeof(int) + sizeof(byte);

        public ValueIndexWriter(Stream stream)
        {
            _stream = stream;
        }

        public void Flush()
        {
            _stream.Flush();
        }

        /// <summary>
        /// Store address of data block.
        /// </summary>
        /// <param name="offset">Offset of data</param>
        /// <param name="len">Length of data</param>
        /// <param name="dataType">Data type</param>
        /// <returns>Record identifier</returns>
        public long Put(long offset, int len, byte dataType)
        {
            var position = _stream.Position;

            _stream.Write(BitConverter.GetBytes(offset));
            _stream.Write(BitConverter.GetBytes(len));
            _stream.WriteByte(dataType);

            return position == 0 ? 0 : position / _blockSize;
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}
