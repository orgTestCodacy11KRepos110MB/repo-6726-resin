using System.Collections.Generic;

namespace Sir
{
    public interface IField
    {
        long DocumentId { get; set; }
        long KeyId { get; set; }
        string Name { get; }
        IEnumerable<ISerializableVector> Tokens { get; }
        VectorNode Tree { get; }
        object Value { get; set; }
        void Analyze<T>(IModel<T> model, bool label, IStreamDispatcher streamDispatcher);
    }
}