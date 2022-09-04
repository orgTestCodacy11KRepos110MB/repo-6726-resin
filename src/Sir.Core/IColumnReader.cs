using System;

namespace Sir
{
    public interface IColumnReader : IDisposable
    {
        Hit ClosestMatchOrNullScanningAllPages(ISerializableVector vector, IModel model);
        Hit ClosestMatchOrNullStoppingAtBestPage(ISerializableVector vector, IModel model);
    }
}
