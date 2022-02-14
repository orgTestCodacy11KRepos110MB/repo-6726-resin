using Sir.Documents;
using System;

namespace Sir.Search
{
    public class UpdateSession : IDisposable
    {
        private readonly DocumentReader _reader;
        private readonly DocumentWriter _writer;

        public UpdateSession(string directory, ulong collectionId, Database sessionFactory) 
        {
            _reader = new DocumentReader(directory, collectionId, sessionFactory);
            _writer = new DocumentWriter(directory, collectionId, sessionFactory, append:false);
        }

        public void Update(long docId, long keyId, object value)
        {
            var docAddress = _reader.GetDocumentAddress(docId);
            var docMap = _reader.GetDocumentMap(docAddress.offset, docAddress.length);
            (long keyId, long valId) mapping = (-1, -1);
            int mappingIx = 0;

            for (;mappingIx < docMap.Length; mappingIx++)
            {
                var fieldMapping = docMap[mappingIx];

                if (fieldMapping.keyId == keyId)
                {
                    mapping = fieldMapping;
                    break;
                }
            }

            if (mapping.valId == -1)
                throw new Exception($"There was no field with keyId {keyId} in document {docId}.");
            
            if (value is string || value is byte[])
            {
                var valueInfo = _writer.PutValue(mapping.keyId, value, out _);

                _writer.UpdateDocumentMap(docAddress.offset, mappingIx, mapping.keyId, valueInfo.valueId);
            }
            else
            {
                var valueAddress = _reader.GetAddressOfValue(mapping.valId);

                _writer.OverwriteFixedLengthValue(valueAddress.offset, value, value.GetType());
            }
        }

        public virtual void Dispose()
        {
            _writer.Dispose();
            _reader.Dispose();
        }
    }
}