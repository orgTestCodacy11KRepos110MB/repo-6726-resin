using Microsoft.Extensions.Logging;
using Sir.IO;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir
{
    public class IndexWriter : IDisposable
    {
        private readonly string _directory;
        private readonly ulong _collectionId;
        private readonly IStreamDispatcher _sessionFactory;
        private readonly ILogger _logger;
        private readonly IDictionary<(long keyId, string fileExtension), Stream> _streams;

        public IndexWriter(
            string directory,
            ulong collectionId,
            IStreamDispatcher sessionFactory, 
            ILogger logger = null)
        {
            _directory = directory;
            _collectionId = collectionId;
            _sessionFactory = sessionFactory;
            _logger = logger;
            _streams = new Dictionary<(long, string), Stream>();
        }

        public void Dispose()
        {
            foreach (var stream in _streams.Values)
            {
                stream.Dispose();
            }
        }

        public void WriteTrees(IDictionary<long, VectorNode> index)
        {
            foreach (var column in index)
            {
                WriteTree(column.Key, column.Value);
            }
        }

        public void WriteTree(long columnKey, VectorNode tree)
        {
            var time = Stopwatch.StartNew();
            var vectorStream = GetOrCreateAppendStream(columnKey, "vec");
            var postingsStream = GetOrCreateSeekableWritableStream(columnKey, "pos");

            using (var columnWriter = new ColumnWriter(GetOrCreateAppendStream(columnKey, "ix"), keepStreamOpen: true))
            using (var pageIndexWriter = new PageIndexWriter(GetOrCreateAppendStream(columnKey, "ixtp"), keepStreamOpen: true))
            {
                var size = columnWriter.CreatePage(tree, vectorStream, postingsStream, pageIndexWriter);

                if (_logger != null)
                    _logger.LogDebug($"serialized column {columnKey}, weight {tree.Weight} {size} in {time.Elapsed}");
            }
        }

        private Stream GetOrCreateAppendStream(long keyId, string fileExtension)
        {
            Stream stream;
            var key = (keyId, fileExtension);

            if (!_streams.TryGetValue(key, out stream))
            {
               stream = _sessionFactory.CreateAppendStream(_directory, _collectionId, keyId, fileExtension);
                _streams.Add(key, stream);
            }

            return stream;
        }

        private Stream GetOrCreateSeekableWritableStream(long keyId, string fileExtension)
        {
            Stream stream;
            var key = (keyId, fileExtension);

            if (!_streams.TryGetValue(key, out stream))
            {
                stream = _sessionFactory.CreateSeekableWritableStream(_directory, _collectionId, keyId, fileExtension);
                _streams.Add(key, stream);
            }

            return stream;
        }
    }
}