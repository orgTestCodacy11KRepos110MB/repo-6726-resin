using System;

namespace Sir.Search
{
    public interface IColumnReader : IDisposable
    {
        Hit ClosestMatch(ISerializableVector vector, IModel model);
    }
}
