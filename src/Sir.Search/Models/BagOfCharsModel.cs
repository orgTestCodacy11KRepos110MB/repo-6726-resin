﻿using Sir.VectorSpace;
using System.Collections.Generic;

namespace Sir.Search
{
    public class BagOfCharsModel : DistanceCalculator, IModel<string>
    {
        public double IdenticalAngle => 0.998d;
        public double FoldAngle => 0.55d;
        public override int NumOfDimensions => System.Text.Unicode.UnicodeRanges.All.Length;

        public void ExecutePut<T>(VectorNode column, VectorNode node)
        {
            column.MergeOrAdd(node, this);
        }

        public IEnumerable<ISerializableVector> Tokenize(string data)
        {
            var source = data.ToCharArray();

            if (source.Length > 0)
            {
                var embedding = new SortedList<int, float>();

                for (int index = 0; index < source.Length; index++)
                {
                    char c = char.ToLower(source[index]);

                    if (char.IsLetterOrDigit(c))
                    {
                        embedding.AddOrAppendToComponent(c);
                    }
                    else
                    {
                        if (embedding.Count > 0)
                        {
                            var vector = new SerializableVector(
                                embedding,
                                NumOfDimensions);

                            embedding.Clear();
                            yield return vector;
                        }
                    }
                }

                if (embedding.Count > 0)
                {
                    var vector = new SerializableVector(
                                embedding,
                                NumOfDimensions);

                    yield return vector;
                }
            }
        }
    }

    public static class TokenizeOperations
    {
        public static void AddOrAppendToComponent(this SortedList<int, float> vec, int key)
        {
            float v;

            if (vec.TryGetValue(key, out v))
            {
                vec[key] = v + 1;
            }
            else
            {
                vec.Add(key, 1);
            }
        }
    }

    public class BocEmbeddingsModel : DistanceCalculator, IModel<string>
    {
        public double IdenticalAngle => 0.95d;
        public double FoldAngle => 0.75d;
        public override int NumOfDimensions { get; }

        private readonly BagOfCharsModel _wordTokenizer;

        public BocEmbeddingsModel(BagOfCharsModel wordTokenizer)
        {
            _wordTokenizer = wordTokenizer;
            NumOfDimensions = wordTokenizer.NumOfDimensions;
        }

        public void ExecutePut<T>(VectorNode column, VectorNode node)
        {
            column.Build(node, this);
        }

        public IEnumerable<ISerializableVector> Tokenize(string data)
        {
            return _wordTokenizer.Tokenize(data);
        }

        public class ContinuousBagOfWordsModel : DistanceCalculator, IModel<string>
        {
            public double IdenticalAngle => 0.95d;
            public double FoldAngle => 0.75d;
            public override int NumOfDimensions { get; }

            private readonly BagOfCharsModel _wordTokenizer;

            public ContinuousBagOfWordsModel(BagOfCharsModel wordTokenizer)
            {
                _wordTokenizer = wordTokenizer;
                NumOfDimensions = wordTokenizer.NumOfDimensions * 3;
            }

            public void ExecutePut<T>(VectorNode column, VectorNode node)
            {
                column.MergeOrAdd(node, this);
            }

            public IEnumerable<ISerializableVector> Tokenize(string data)
            {
                var tokens = (IList<ISerializableVector>)_wordTokenizer.Tokenize(data);

                for (int i = 0; i < tokens.Count; i++)
                {
                    var context0 = i - 1;
                    var context1 = i + 1;
                    var token = tokens[i];
                    var vector = new SerializableVector(NumOfDimensions, token.Label);

                    if (context0 >= 0)
                    {
                        vector.AddInPlace(tokens[context0].Shift(0, NumOfDimensions));
                    }

                    if (context1 < tokens.Count)
                    {
                        vector.AddInPlace(tokens[context1].Shift(_wordTokenizer.NumOfDimensions * 2, NumOfDimensions));
                    }

                    if (vector.ComponentCount == 0)
                    {
                        yield return token.Shift(_wordTokenizer.NumOfDimensions, NumOfDimensions);
                    }
                    else
                    {
                        yield return vector;
                    }
                }
            }
        }
    }
}