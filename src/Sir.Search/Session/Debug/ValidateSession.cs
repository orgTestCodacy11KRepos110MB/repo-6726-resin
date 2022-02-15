using System;

namespace Sir.Search
{
    /// <summary>
    /// Validate a collection.
    /// </summary>
    public class ValidateSession<T> : IDisposable
    {
        public ulong CollectionId { get; }

        private readonly SearchSession _readSession;
        private readonly QueryParser<T> _queryParser;

        public ValidateSession(
            ulong collectionId,
            SearchSession searchSession,
            QueryParser<T> queryParser
            )
        {
            CollectionId = collectionId;
            _readSession = searchSession;
            _queryParser = queryParser;
        }

        public void Validate(Document doc)
        {
            foreach (var field in doc.Fields)
            {
                var query = _queryParser.Parse(CollectionId, (T)field.Value, field.Name, field.Name, true, false, true);
                var result = _readSession.SearchScalar(query);

                if (doc.Id != result.Id)
                {
                    throw new Exception($"unable to validate doc.Id {doc.Id}");
                }
            }
        }

        public void Dispose()
        {
            _readSession.Dispose();
        }
    }
}