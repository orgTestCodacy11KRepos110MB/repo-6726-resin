namespace Sir.Strings
{
    public class OptimizedPageIndexingStrategy : IIndexingStrategy
    {
        private readonly IModel _model;

        public OptimizedPageIndexingStrategy(IModel model)
        {
            _model = model;
        }

        public void ExecutePut<T>(VectorNode column, VectorNode node, IColumnReader reader)
        {
            var existing = reader.ClosestMatchOrNull(node.Vector, _model);

            if (existing != null && existing.Score >= _model.IdenticalAngle)
            {
                node.PostingsOffset = existing.Node.PostingsOffset;
            }

            column.AddOrAppend(node, _model);
        }
    }
}