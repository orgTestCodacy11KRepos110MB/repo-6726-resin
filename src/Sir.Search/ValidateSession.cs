﻿using System;

namespace Sir
{
    /// <summary>
    /// Validate a collection.
    /// </summary>
    public class ValidateSession<T> : IDisposable
    {
        public ulong CollectionId { get; }

        private readonly SearchSession _searchSession;
        private readonly QueryParser<T> _queryParser;

        public ValidateSession(
            ulong collectionId,
            SearchSession searchSession,
            QueryParser<T> queryParser
            )
        {
            CollectionId = collectionId;
            _searchSession = searchSession;
            _queryParser = queryParser;
        }

        public void Validate(Document doc)
        {
            foreach (var field in doc.Fields)
            {
                if (string.IsNullOrWhiteSpace(field.Value.ToString()))
                    throw new ArgumentNullException(nameof(field));

                var query = _queryParser.Parse(CollectionId, (T)field.Value, field.Name, field.Name, true, false, true);
                var result = _searchSession.SearchIdentical(query, 1000);
                bool isMatch = false;

                foreach (var document in result.Documents)
                {
                    if (doc.Id == document.Id)
                    {
                        isMatch = true;
                        break;
                    }
                }
                
                if (!isMatch)
                {
                    if (result.Total == 0)
                    {
                        throw new Exception($"unable to validate doc.Id {doc.Id} because no documents were found. field value: {field.Value}");
                    }
                    else
                    {
                        throw new Exception($"unable to validate doc.Id {doc.Id} because wrong document was found. field value: {field.Value}");
                    }
                }
            }
        }

        public void Dispose()
        {
            _searchSession.Dispose();
        }
    }
}