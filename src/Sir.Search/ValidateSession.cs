using System;

namespace Sir
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

                if (result == null)
                {
                    throw new Exception($"unable to validate doc.Id {doc.Id} because no matching document was found. Term value: {field.Value}");
                }
                else if (doc.Id != result.Id)
                {
                    throw new Exception($"unable to validate doc.Id {doc.Id} because wrong document was found. Term value: {field.Value}. Document value: {result.Get(field.Name)}");
                }
            }
        }

        public void Dispose()
        {
            _readSession.Dispose();
        }
    }
}