using Sir.KeyValue;
using System;
using System.Collections.Generic;

namespace Sir.Documents
{
    /// <summary>
    /// Writes documents to a database.
    /// </summary>
    public class DocumentWriter : KeyValueWriter, IDisposable
    {
        private readonly DocMapWriter _docs;
        private readonly DocIndexWriter _docIx;
        
        public DocumentWriter(string directory, ulong collectionId, IStreamDispatcher database, bool append = true) : base(directory, collectionId, database, append)
        {
            var docStream = append ? database.CreateAppendStream(directory, collectionId, "docs") : database.CreateSeekableWritableStream(directory, collectionId, "docs");
            var docIndexStream = database.CreateAppendStream(directory, collectionId, "dix");

            _docs = new DocMapWriter(docStream);
            _docIx = new DocIndexWriter(docIndexStream);
        }

        public long IncrementDocId()
        {
            return _docIx.IncrementDocId();
        }

        public (long offset, int length) PutDocumentMap(IList<(long keyId, long valId)> doc)
        {
            return _docs.Put(doc);
        }

        public void UpdateDocumentMap(long offsetOfMap, int indexInMap, long keyId, long valId)
        {
            _docs.Overwrite(offsetOfMap, indexInMap, keyId, valId);
        }

        public void PutDocumentAddress(long docId, long offset, int len)
        {
            _docIx.Put(docId, offset, len);
        }

        public override void Dispose()
        {
            base.Dispose();

            _docs.Dispose();
            _docIx.Dispose();
        }
    }
}
