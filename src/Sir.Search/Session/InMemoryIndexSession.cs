using System;
using System.Collections.Generic;

namespace Sir.Search
{
    public class InMemoryIndexSession<T> : IIndexSession, IDisposable
    {
        private readonly IModel<T> _model;
        private readonly IIndexingStrategy _indexingStrategy;
        private readonly IDictionary<long, VectorNode> _index;

        public InMemoryIndexSession(
            IModel<T> model,
            IIndexingStrategy indexingStrategy)
        {
            _model = model;
            _indexingStrategy = indexingStrategy;
            _index = new Dictionary<long, VectorNode>();
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
                _indexingStrategy.ExecutePut<T>(column, new VectorNode(node.Vector, docIds: node.DocIds));
            }
        }

        public IndexInfo GetIndexInfo()
        {
            return new IndexInfo(GetGraphInfo());
        }

        public IDictionary<long, VectorNode> GetInMemoryIndices()
        {
            return _index;
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
        }
    }

    public class EmbeddSession<T> : IDisposable
    {
        private readonly IModel<T> _model;
        private readonly IIndexingStrategy _indexingStrategy;
        private readonly IDictionary<long, VectorNode> _index;

        public EmbeddSession(
            IModel<T> model,
            IIndexingStrategy indexingStrategy)
        {
            _model = model;
            _indexingStrategy = indexingStrategy;
            _index = new Dictionary<long, VectorNode>();
        }

        public void Put(long docId, long keyId, T value, bool label)
        {
            var vectors = _model.CreateEmbedding(value, label);
            VectorNode column;

            if (!_index.TryGetValue(keyId, out column))
            {
                column = new VectorNode();
                _index.Add(keyId, column);
            }

            foreach (var vector in vectors)
            {
                _indexingStrategy.ExecutePut<T>(column, new VectorNode(vector, docId));
            }

            var size = PathFinder.Size(column);


        }

        public IndexInfo GetIndexInfo()
        {
            return new IndexInfo(GetGraphInfo());
        }

        public IDictionary<long, VectorNode> GetInMemoryIndex()
        {
            return _index;
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
        }
    }
}