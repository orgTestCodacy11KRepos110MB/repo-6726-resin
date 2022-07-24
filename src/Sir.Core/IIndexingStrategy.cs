namespace Sir
{
    public interface IIndexingStrategy
    {
        void ExecutePut<T>(VectorNode column, VectorNode node, IColumnReader reader);
    }
}
