using System;

namespace Sir.Search
{
    public interface IColumnReader : IDisposable
    {
        Hit ClosestMatchOrNull(ISerializableVector vector, IModel model);
    }
}
