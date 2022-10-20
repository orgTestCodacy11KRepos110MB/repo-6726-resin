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
                var result = _readSession.SearchIdentical(query, 100);

                if (result == null)
                {
                    throw new Exception($"unable to validate doc.Id {doc.Id} because no matching document was found. Term value: {field.Value}");
                }

                bool isMatch = false;

                foreach (var document in result.Documents)
                {
                    if (doc.Id == document.Id)
                    {
                        isMatch = true;
                    }
                }
                
                if (!isMatch)
                {
                    throw new Exception($"unable to validate doc.Id {doc.Id} because wrong document was found.");
                }
            }
        }

        public void Dispose()
        {
            _readSession.Dispose();
        }
    }
}