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
            var tree = new VectorNode(keyId: keyId);

            foreach (var token in tokens)
            {
                tree.AddIfUnique(new VectorNode(token, docId: docId, keyId: keyId), _model);
            }

            Put(tree);
        }

        public void Put(VectorNode tree)
        {
            VectorNode column;

            if (!_index.TryGetValue(tree.KeyId.Value, out column))
            {
                column = new VectorNode();
                _index.Add(tree.KeyId.Value, column);
            }

            foreach (var node in PathFinder.All(tree))
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

    public class OptimizedPagingIndexingStrategy : IIndexingStrategy
    {
        private readonly ColumnReader _columnReader;
        private readonly IModel _model;

        public OptimizedPagingIndexingStrategy(ColumnReader columnReader, IModel model)
        {
            _columnReader = columnReader;
            _model = model;
        }

        public void ExecutePut<T>(VectorNode column, VectorNode node)
        {
            var existing = _columnReader.ClosestMatchOrNull(node.Vector, _model);

            if (existing != null && existing.Score >= _model.IdenticalAngle)
            {
                node.PostingsOffset = existing.Node.PostingsOffset;
            }

            column.AddOrAppend(node, _model);
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