using NUnit.Framework;
using Sir.Mnist;
using Sir.Search;
using Sir.VectorSpace;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Sir.Tests
{
    public class ImageModelTests
    {
        [Test]
        public void Can_create_in_memory_linear_classifier()
        {
            // Use the same set of images to both create and validate a linear classifier.

            var trainingData = new MnistReader(
                @"resources\t10k-images.idx3-ubyte",
                @"resources\t10k-labels.idx1-ubyte").Read().Take(100).ToArray();

            var model = new LinearClassifierImageModel();
            var index = model.CreateTree(model, trainingData);

            Print(index);

            Assert.DoesNotThrow(() =>
            {
                var count = 0;
                var errors = 0;

                foreach (var image in trainingData)
                {
                    foreach (var queryVector in model.CreateEmbedding(image, true))
                    {
                        var hit = PathFinder.ClosestMatch(index, queryVector, model);

                        if (hit == null)
                        {
                            throw new Exception($"unable to find {image} in index.");
                        }

                        if (!hit.Node.Vector.Label.Equals(image.Label))
                        {
                            errors++;
                        }

                        Debug.WriteLine($"{image} matched with {hit.Node.Vector.Label} with {hit.Score * 100}% certainty.");

                        count++;
                    }
                }

                var errorRate = (float)errors / count;

                if (errorRate > 0)
                {
                    throw new Exception($"error rate: {errorRate * 100}%. too many errors.");
                }

                Debug.WriteLine($"error rate: {errorRate}");
            });
        }

        private static void Print(VectorNode tree)
        {
            var diagram = PathFinder.Visualize(tree);
            File.WriteAllText("imagemodeltesttree.txt", diagram);
            Debug.WriteLine(diagram);
        }
    }
}