using Sir.IO;
using System.Collections.Generic;

namespace Sir.Strings
{
    public class BagOfCharsModel : DistanceCalculator, IModel<string>
    {
        public double IdenticalAngle => 0.998d;
        public double FoldAngle => 0.55d;
        public override int NumOfDimensions => System.Text.Unicode.UnicodeRanges.All.Length;

        public void ExecutePut<T>(VectorNode column, VectorNode node, IColumnReader reader)
        {
            column.AddOrAppend(node, this);
        }

        public IEnumerable<ISerializableVector> CreateEmbedding(string data, bool label)
        {
            var source = data.ToCharArray();

            if (source.Length > 0)
            {
                var embedding = new SortedList<int, float>();
                var offset = 0;
                int index = 0;

                for (; index < source.Length; index++)
                {
                    char c = char.ToLower(source[index]);

                    if (char.IsLetterOrDigit(c))
                    {
                        embedding.AddOrAppendToComponent(c, 1);
                    }
                    else
                    {
                        if (embedding.Count > 0)
                        {
                            var len = index - offset;

                            var vector = new SerializableVector(
                                embedding,
                                NumOfDimensions,
                                label ? new string(source, offset, len) : null);

                            embedding.Clear();
                            yield return vector;
                        }

                        offset = index + 1;
                    }
                }

                if (embedding.Count > 0)
                {
                    var len = index - offset;

                    var vector = new SerializableVector(
                                embedding,
                                NumOfDimensions,
                                label ? new string(source, offset, len) : null);

                    yield return vector;
                }
            }
        }
    }

    public static class TokenizeOperations
    {
        public static void AddOrAppendToComponent(this SortedList<int, float> vec, int key, float value)
        {
            float v;

            if (vec.TryGetValue(key, out v))
            {
                vec[key] = v + value;
            }
            else
            {
                vec.Add(key, value);
            }
        }
    }
}