using System;

namespace Sir.KeyValue
{
    /// <summary>
    /// Writes keys and values to a database.
    /// </summary>
    public class KeyValueWriter : IDisposable
    {
        private readonly ValueWriter _vals;
        private readonly ValueWriter _keys;
        private readonly ValueIndexWriter _valIx;
        private readonly ValueIndexWriter _keyIx;
        private readonly ulong _collectionId;
        private readonly IStreamDispatcher _database;
        private readonly string _directory;
        private readonly object _keyLock = new object();
        
        public KeyValueWriter(string directory, ulong collectionId, IStreamDispatcher database, bool append = true)
            : this(
            new ValueWriter(append? database.CreateAppendStream(directory, collectionId, "val") : database.CreateSeekableWritableStream(directory, collectionId, "val")),
            new ValueWriter(database.CreateAppendStream(directory, collectionId, "key")),
            new ValueIndexWriter(database.CreateAppendStream(directory, collectionId, "vix")),
            new ValueIndexWriter(database.CreateAppendStream(directory, collectionId, "kix"))
            )
        {
            _collectionId = collectionId;
            _database = database;
            _directory = directory;
        }

        public KeyValueWriter(ValueWriter values, ValueWriter keys, ValueIndexWriter valIx, ValueIndexWriter keyIx)
        {
            _vals = values;
            _keys = keys;
            _valIx = valIx;
            _keyIx = keyIx;
        }

        public long EnsureKeyExistsSafely(string keyStr)
        {
            var keyHash = keyStr.ToHash();
            long keyId;

            if (!_database.TryGetKeyId(_directory, _collectionId, keyHash, out keyId))
            {
                lock (_keyLock)
                {
                    if (!_database.TryGetKeyId(_directory, _collectionId, keyHash, out keyId))
                    {
                        // We have a new key!

                        // store key
                        var keyInfo = PutKey(keyStr);

                        keyId = PutKeyInfo(keyInfo.offset, keyInfo.len, keyInfo.dataType);

                        // store key mapping
                        _database.RegisterKeyMapping(_directory, _collectionId, keyHash, keyId);
                    }
                }
            }

            return keyId;
        }

        public long EnsureKeyExists(string keyStr)
        {
            var keyHash = keyStr.ToHash();
            long keyId;

            if (!_database.TryGetKeyId(_directory, _collectionId, keyHash, out keyId))
            {
                // We have a new key!

                // store key
                var keyInfo = PutKey(keyStr);

                keyId = PutKeyInfo(keyInfo.offset, keyInfo.len, keyInfo.dataType);

                // store key mapping
                _database.RegisterKeyMapping(_directory, _collectionId, keyHash, keyId);
            }

            return keyId;
        }

        public (long keyId, long valueId) PutValue(long keyId, object val, out byte dataType)
        {
            // store value
            var valInfo = PutValue(val);
            var valId = PutValueInfo(valInfo.offset, valInfo.len, valInfo.dataType);

            dataType = valInfo.dataType;

            // return refs to key and value
            return (keyId, valId);
        }

        public (long offset, int len, byte dataType) PutKey(object value)
        {
            return _keys.Put(value);
        }

        public (long offset, int len, byte dataType) PutValue(object value)
        {
            return _vals.Put(value);
        }

        public long PutKeyInfo(long offset, int len, byte dataType)
        {
            return _keyIx.Put(offset, len, dataType);
        }

        public long PutValueInfo(long offset, int len, byte dataType)
        {
            return _valIx.Put(offset, len, dataType);
        }

        public void OverwriteFixedLengthValue(long offset, object value, Type type)
        {
            if (type == typeof(string) || type == typeof(byte[]))
                throw new InvalidOperationException();

            _vals.Stream.Seek(offset, System.IO.SeekOrigin.Begin);
            _vals.Put(value);
        }

        public virtual void Dispose()
        {
            _vals.Dispose();
            _keys.Dispose();
            _valIx.Dispose();
            _keyIx.Dispose();
        }
    }
}
