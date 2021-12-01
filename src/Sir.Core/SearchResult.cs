using System.Collections.Generic;

namespace Sir
{
    public class SearchResult
    {
        public IQuery Query { get; }
        public long Total { get; }
        public IEnumerable<IDocument> Documents { get; }
        public int Count { get; }

        public SearchResult(IQuery query, long total, int count, IEnumerable<IDocument> documents)
        {
            Query = query;
            Total = total;
            Count = count;
            Documents = documents;
        }
    }
}
