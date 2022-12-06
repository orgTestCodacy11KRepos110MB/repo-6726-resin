using Sir.IO;
using System;
using System.Collections.Generic;

namespace Sir
{
    public class IndexSession<T> : IIndexSession<T>, IDisposable
    {
        private readonly IModel<T> _model;
        private readonly IIndexReadWriteStrategy _indexingStrategy;
        private readonly IDictionary<long, VectorNode> _index;
        private readonly IDictionary<long, IColumnReader> _readers;
        private readonly IStreamDispatcher _sessionFactory;
        private readonly string _directory;
        private readonly ulong _collectionId;

        public IndexSession(
            IModel<T> model,
            IIndexReadWriteStrategy indexingStrategy,
            IStreamDispatcher sessionFactory, 
            string directory,
            ulong collectionId)
        {
            _model = model;
            _indexingStrategy = indexingStrategy;
            _index = new Dictionary<long, VectorNode>();
            _readers = new Dictionary<long, IColumnReader>();
            _sessionFactory = sessionFactory;
            _directory = directory;
            _collectionId = collectionId;
        }

        public void Put(long docId, long keyId, T value, bool label)
        {
            var tokens = _model.CreateEmbedding(value, label);

            Put(docId, keyId, tokens);
        }

        public void Put(long docId, long keyId, IEnumerable<ISerializableVector> tokens)
        {
            var documentTree = new VectorNode(keyId: keyId);

            foreach (var token in tokens)
            {
                documentTree.AddIfUnique(new VectorNode(token, docId: docId, keyId: keyId), _model);
            }

            Put(documentTree);
        }

        public void Put(VectorNode documentTree)
        {
            VectorNode column;

            if (!_index.TryGetValue(documentTree.KeyId.Value, out column))
            {
                column = new VectorNode();
                _index.Add(documentTree.KeyId.Value, column);
            }

            foreach (var node in PathFinder.All(documentTree))
            {
                _indexingStrategy.Put<T>(
                    column, 
                    new VectorNode(node.Vector, docIds: node.DocIds), 
                    GetReader(documentTree.KeyId.Value));
            }
        }

        public IndexInfo GetIndexInfo()
        {
            return new IndexInfo(GetGraphInfo());
        }

        public void Commit(IndexWriter indexWriter)
        {
            indexWriter.Commit(_index);
        }

        public IDictionary<long, VectorNode> GetInMemoryIndices()
        {
            return _index;
        }

        private IColumnReader GetReader(long keyId)
        {
            IColumnReader reader;

            if (!_readers.TryGetValue(keyId, out reader))
            {
                reader = _sessionFactory.CreateColumnReader(_directory, _collectionId, keyId);
                _readers.Add(keyId, reader);
            }

            return reader;
        }

        private IEnumerable<GraphInfo> GetGraphInfo()
        {
            foreach (var ix in _index)
            {
                yield return new GraphInfo(ix.Key, ix.Value);
            }
        }

        public void Dispose()
        {
            foreach (var reader in _readers.Values)
                reader.Dispose();
        }
    }
}