using System;

namespace Sir.VectorSpace
{
    public interface IColumnReader : IDisposable
    {
        Hit ClosestMatch(ISerializableVector vector, IModel model);
    }
}
