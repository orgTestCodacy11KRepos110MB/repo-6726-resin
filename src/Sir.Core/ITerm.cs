using System.Collections.Generic;

namespace Sir
{
    public interface ITerm
    {
        ulong CollectionId { get; }
        string Directory { get; }
        string Key { get; }
        long KeyId { get; }
        long PostingsOffset { get; set; }
        IList<(ulong collectionId, long documentId)> DocumentIds { get; set; }
        double Score { get; set; }
        ISerializableVector Vector { get; }
        bool IsIntersection { get; set; }
        bool IsUnion { get; set; }
    }
}