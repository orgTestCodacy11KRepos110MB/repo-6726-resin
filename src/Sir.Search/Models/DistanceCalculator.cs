using MathNet.Numerics.LinearAlgebra;
using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.Search
{
    public abstract class DistanceCalculator : IDistanceCalculator
    {
        public abstract int NumOfDimensions { get; }

        public double CosAngle(ISerializableVector vec1, ISerializableVector vec2)
        {
            var dotProduct = vec1.Value.DotProduct(vec2.Value);

            if (dotProduct == 0)
                return 0;

            var dotSelf1 = vec1.Value.Norm(2);
            var dotSelf2 = vec2.Value.Norm(2);

            var cosineDistance = dotProduct / (dotSelf1 * dotSelf2);

            return cosineDistance;
        }

        public double CosAngle(ISerializableVector vector, long vectorOffset, int componentCount, Stream vectorStream)
        {
            Span<byte> buf = new byte[componentCount * 2 * sizeof(int)];

            vectorStream.Seek(vectorOffset, SeekOrigin.Begin);
            vectorStream.Read(buf);

            var index = MemoryMarshal.Cast<byte, int>(buf.Slice(0, componentCount * sizeof(int)));
            var values = MemoryMarshal.Cast<byte, float>(buf.Slice(componentCount * sizeof(float)));
            var tuples = new Tuple<int, float>[componentCount];

            for (int i = 0; i < componentCount; i++)
            {
                tuples[i] = new Tuple<int, float>(index[i], values[i]);
            }

            var vectorOnFile = CreateVector.SparseOfIndexed(NumOfDimensions, tuples);

            var dotProduct = vector.Value.DotProduct(vectorOnFile);

            if (dotProduct == 0)
                return 0;

            var dotSelf1 = vector.Value.Norm(2);
            var dotSelf2 = vectorOnFile.Norm(2);

            return dotProduct / (dotSelf1 * dotSelf2);
        }
    }
}
