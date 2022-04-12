using System;

namespace Sir.Search
{
    public interface ISearchSession : IDisposable
    {
        SearchResult Search(IQuery query, int skip, int take);
        Document SearchScalar(IQuery query);
    }
}