using System;

namespace Sir.Strings
{
    public interface ISearchSession : IDisposable
    {
        SearchResult Search(IQuery query, int skip, int take);
        Document SearchScalar(IQuery query);
    }
}