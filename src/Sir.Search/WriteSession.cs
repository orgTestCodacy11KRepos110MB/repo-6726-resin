﻿using Sir.Documents;
using System;
using System.Collections.Generic;

namespace Sir
{
    /// <summary>
    /// Write session targeting a single collection.
    /// </summary>
    public class WriteSession : IDisposable
    {
        private readonly DocumentWriter _streamWriter;

        public WriteSession(
            DocumentWriter streamWriter)
        {
            _streamWriter = streamWriter;
        }

        public void Put(IDocument document)
        {
            var docMap = new List<(long keyId, long valId)>();

            document.Id = _streamWriter.IncrementDocId();

            foreach (var field in document.Fields)
            {
                field.DocumentId = document.Id;

                if (field.Value != null)
                {
                    Write(field, docMap);
                }
                else
                {
                    continue;
                }
            }

            var docMeta = _streamWriter.PutDocumentMap(docMap);

            _streamWriter.PutDocumentAddress(document.Id, docMeta.offset, docMeta.length);
        }

        private void Write(IField field, IList<(long, long)> docMap)
        {
            field.KeyId = EnsureKeyExists(field.Name);

            Write(field.KeyId, field.Value, docMap);
        }

        private void Write(string key, object val, IList<(long, long)> docMap)
        {
            var keyId = EnsureKeyExists(key);

            Write(keyId, val, docMap);
        }

        private void Write(long keyId, object val, IList<(long, long)> docMap)
        {
            // store value
            var kvmap = _streamWriter.PutValue(keyId, val, out _);

            // store refs to k/v pair
            docMap.Add(kvmap);
        }

        public long EnsureKeyExists(string key)
        {
            return _streamWriter.EnsureKeyExists(key);
        }

        public long EnsureKeyExistsSafely(string key)
        {
            return _streamWriter.EnsureKeyExistsSafely(key);
        }

        public void Dispose()
        {
            _streamWriter.Dispose();
        }
    }
}