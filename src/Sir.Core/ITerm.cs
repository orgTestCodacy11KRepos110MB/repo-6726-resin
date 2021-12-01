using System.Collections.Generic;

namespace Sir
{
    public interface ITerm
    {
        ulong CollectionId { get; }
        string Directory { get; }
        string Key { get; }
        long KeyId { get; }
        object Label { get; }
        IList<long> PostingsOffsets { get; set; }
        IList<(ulong collectionId, long documentId)> Result { get; set; }
        double Score { get; set; }
        ISerializableVector Vector { get; }
        bool IsIntersection { get; set; }
        bool IsUnion { get; set; }
    }
}