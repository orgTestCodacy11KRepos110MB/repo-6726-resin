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
        }

        public void Dispose()
        {
        }

        public void Commit(IDictionary<long, VectorNode> index)
        {
            foreach (var column in index)
            {
                Commit(column.Key, column.Value);
            }
        }

        public void Commit(long keyId, VectorNode tree)
        {
            var time = Stopwatch.StartNew();
            using(var vectorStream = CreateAppendStream(keyId, "vec"))
            using (var postingsStream = CreateAppendStream(keyId, "pos"))
            using (var columnWriter = new ColumnWriter(CreateAppendStream(keyId, "ix")))
            using (var pageIndexWriter = new PageIndexWriter(CreateAppendStream(keyId, "ixtp")))
            {
                var size = columnWriter.CreatePage(tree, vectorStream, postingsStream, pageIndexWriter);

                if (_logger != null)
                    _logger.LogDebug($"serialized column {keyId}, weight {tree.Weight} {size} in {time.Elapsed}");
            }
        }

        private Stream CreateAppendStream(long keyId, string fileExtension)
        {
            return _sessionFactory.CreateAppendStream(_directory, _collectionId, keyId, fileExtension);
        }
    }
}