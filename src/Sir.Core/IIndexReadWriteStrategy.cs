namespace Sir
{
    public interface IIndexReadWriteStrategy
    {
        void Put<T>(VectorNode column, VectorNode node, IColumnReader reader);
        Hit GetClosestMatchOrNull(ISerializableVector vector, IModel model, IColumnReader reader);
    }
}
