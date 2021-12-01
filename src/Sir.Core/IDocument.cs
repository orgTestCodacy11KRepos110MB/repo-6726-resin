using System.Collections.Generic;

namespace Sir
{
    public interface IDocument
    {
        ulong CollectionId { get; set; }
        IList<IField> Fields { get; set; }
        long Id { get; set; }
        double Score { get; set; }

        IField Get(string key);
        bool TryGetValue(string key, out IField value);
    }
}