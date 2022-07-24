namespace Sir.Search
{
    public class OptimizedPageIndexingStrategy : IIndexingStrategy
    {
        private readonly ColumnReader _columnReader;
        private readonly IModel _model;

        public OptimizedPageIndexingStrategy(ColumnReader columnReader, IModel model)
        {
            _columnReader = columnReader;
            _model = model;
        }

        public void ExecutePut<T>(VectorNode column, VectorNode node)
        {
            var existing = _columnReader.ClosestMatchOrNull(node.Vector, _model);

            if (existing == null || existing.Score < _model.IdenticalAngle)
            {
                column.AddOrAppend(node, _model);
            }
            else
            {
                node.PostingsOffset = existing.Node.PostingsOffset;
            }
        }
    }
}