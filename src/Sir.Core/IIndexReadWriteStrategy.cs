namespace Sir
{
    public interface IIndexReadWriteStrategy
    {
        void ExecutePut<T>(VectorNode column, VectorNode node, IColumnReader reader);
        Hit ExecuteGetClosestMatchOrNull(ISerializableVector vector, IModel model, IColumnReader reader);
    }
}
