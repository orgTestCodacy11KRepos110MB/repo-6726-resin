﻿using Microsoft.Extensions.Logging;
using Sir.VectorSpace;
using System;
using System.Collections.Generic;

namespace Sir.Search
{
    public class IndexFileStreamProvider : IDisposable
    {
        private readonly ulong _collectionId;
        private readonly SessionFactory _sessionFactory;
        private readonly ILogger _logger;
        private readonly System.IO.Stream _postingsStream;
        private readonly System.IO.Stream _vectorStream;
        private readonly bool _keepStreamsOpen;

        public IndexFileStreamProvider(
            ulong collectionId, 
            SessionFactory sessionFactory, 
            bool keepStreamsOpen = false, 
            ILogger logger = null)
        {
            _collectionId = collectionId;
            _sessionFactory = sessionFactory;
            _logger = logger??sessionFactory.Logger;
            _postingsStream = _sessionFactory.CreateAppendStream(_collectionId, "pos");
            _vectorStream = _sessionFactory.CreateAppendStream(_collectionId, "vec");
            _keepStreamsOpen = keepStreamsOpen;
        }

        public void Dispose()
        {
            _postingsStream.Flush();
            _vectorStream.Flush();

            if (!_keepStreamsOpen)
            {
                _postingsStream.Dispose();
                _vectorStream.Dispose();
            }
        }

        public void Write(IDictionary<long, VectorNode> index)
        {
            using (var postingsStream = _sessionFactory.CreateAppendStream(_collectionId, "pos"))
            using (var vectorStream = _sessionFactory.CreateAppendStream(_collectionId, "vec"))
            {
                foreach (var column in index)
                {
                    using (var indexStream = _sessionFactory.CreateAppendStream(_collectionId, column.Key, "ix"))
                    using (var columnWriter = new ColumnStreamWriter(indexStream))
                    using (var pageIndexWriter = new PageIndexWriter(_sessionFactory.CreateAppendStream(_collectionId, column.Key, "ixtp")))
                    {
                        var size = columnWriter.CreatePage(column.Value, vectorStream, postingsStream, pageIndexWriter);

                        if (_logger != null)
                            _logger.LogInformation($"serialized column {column.Key}, weight {column.Value.Weight} {size}");
                    }
                }
            }
        }

        public void WriteOneHotVectors(IDictionary<long, VectorNode> index)
        {
            foreach (var column in index)
            {
                var matrix = PathFinder.AsOneHotMatrix(column.Value);

                using (var vectorStream = _sessionFactory.CreateAppendStream(_collectionId, column.Key, "1h.vec"))
                {
                    foreach (var row in matrix)
                    {
                        var vector = new IndexedVector(row);
                        vector.Serialize(vectorStream);
                    }
                }
            }
        }
    }
}