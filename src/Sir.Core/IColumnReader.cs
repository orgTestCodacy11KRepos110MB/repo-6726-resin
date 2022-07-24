using System;

namespace Sir
{
    public interface IColumnReader : IDisposable
    {
        Hit ClosestMatchOrNull(ISerializableVector vector, IModel model);
    }
}
