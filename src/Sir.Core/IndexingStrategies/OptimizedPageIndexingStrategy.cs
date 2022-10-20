using Sir.IO;

namespace Sir
{
    public class OptimizedPageIndexingStrategy : IIndexReadWriteStrategy
    {
        private readonly IModel _model;

        public OptimizedPageIndexingStrategy(IModel model)
        {
            _model = model;
        }

        public Hit GetClosestMatchOrNull(ISerializableVector vector, IModel model, IColumnReader reader)
        {
            return reader.ClosestMatchOrNullStoppingAtBestPage(vector, model);
        }

        public void Put<T>(VectorNode column, VectorNode node, IColumnReader reader)
        {
            var existing = reader.ClosestMatchOrNullStoppingAtBestPage(node.Vector, _model);

            if (existing != null && existing.Score >= _model.IdenticalAngle)
            {
                node.PostingsOffset = existing.Node.PostingsOffset;
            }

            column.AddOrAppend(node, _model);
        }
    }

    public class NonOptimizedPageIndexingStrategy : IIndexReadWriteStrategy
    {
        private readonly IModel _model;

        public NonOptimizedPageIndexingStrategy(IModel model)
        {
            _model = model;
        }

        public Hit GetClosestMatchOrNull(ISerializableVector vector, IModel model, IColumnReader reader)
        {
            return reader.ClosestMatchOrNullScanningAllPages(vector, model);
        }

        public void Put<T>(VectorNode column, VectorNode node, IColumnReader reader)
        {
            column.AddOrAppend(node, _model);
        }
    }
}