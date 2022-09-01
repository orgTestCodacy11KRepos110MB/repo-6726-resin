using Sir.IO;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir
{
    [DebuggerDisplay("{Name}")]
    public class Field : IField
    {
        private IEnumerable<ISerializableVector> _tokens;

        public VectorNode Tree { get; private set; }
        public long KeyId { get; set; }
        public long DocumentId { get; set; }
        public string Name { get; }
        public object Value { get; set; }
        public IEnumerable<ISerializableVector> Tokens { get { return _tokens; } }

        public Field(string name, object value, long keyId = -1, long documentId = -1)
        {
            if (name is null) throw new ArgumentNullException(nameof(name));
            if (value == null) throw new ArgumentNullException(nameof(value));

            Name = name;
            Value = value;
            KeyId = keyId;
            DocumentId = documentId;
        }

        private IEnumerable<ISerializableVector> GetTokens()
        {
            foreach (var node in PathFinder.All(Tree))
                yield return node.Vector;
        }

        public void Analyze<T>(IModel<T> model, bool label, IStreamDispatcher streamDispatcher)
        {
            var tokens = model.CreateEmbedding((T)Value, label);

            Tree = new VectorNode();

            using (var reader = streamDispatcher.CreateColumnReader("", 0, 0))
            {
                foreach (var token in tokens)
                {
                    model.ExecutePut<string>(Tree, new VectorNode(token, keyId: KeyId), reader);
                }

                _tokens = GetTokens();
            }
                
        }
    }
}