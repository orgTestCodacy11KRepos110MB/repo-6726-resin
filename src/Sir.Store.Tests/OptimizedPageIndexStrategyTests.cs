using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Sir.Strings;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Sir.Tests
{
    public class OptimizedPageIndexStrategyTests
    {
        private ILoggerFactory _loggerFactory;
        private SessionFactory _sessionFactory;

        private readonly string[] _data = new string[] { "Ferriman–Gallwey score", "apples", "apricote", "apricots", "avocado", "avocados", "banana", "bananas", "blueberry", "blueberries", "cantalope" };

        [Test]
        public void Can_traverse_index_in_memory()
        {
            var model = new BagOfCharsModel();

            using (var reader = _sessionFactory.CreateColumnReader("", 0, 0))
            {
                var index = model.CreateTree(model, reader, _data);

                Debug.WriteLine(PathFinder.Visualize(index));

                Assert.DoesNotThrow(() =>
                {
                    foreach (var word in _data)
                    {
                        foreach (var queryVector in model.CreateEmbedding(word, true))
                        {
                            var hit = PathFinder.ClosestMatch(index, queryVector, model);

                            if (hit == null)
                            {
                                throw new Exception($"unable to find {word} in index.");
                            }

                            if (hit.Score < model.IdenticalAngle)
                            {
                                throw new Exception($"unable to score {word}.");
                            }

                            Debug.WriteLine($"{word} matched with {hit.Node.Vector.Label} with {hit.Score * 100}% certainty.");
                        }
                    }
                });
            }
            
        }

        [Test]
        public void Can_traverse_streamed()
        {
            var model = new BagOfCharsModel();

            using (var pageIndexStream = new MemoryStream())
            using (var indexStream = new MemoryStream())
            using (var vectorStream = new MemoryStream())
            using (var pageStream = new MemoryStream())
            {
                for (int i = 2; i > 0; i--)
                {
                    var data = _data.Take(_data.Length / i).ToArray();
                    var indexReadStream = new MemoryStream();
                    var vectorReadStream = new MemoryStream();
                    var pageIndexReadStream = new MemoryStream();

                    indexStream.Position = 0;
                    vectorStream.Position = 0;
                    pageIndexStream.Position = 0;

                    indexStream.CopyTo(indexReadStream);
                    vectorStream.CopyTo(vectorReadStream);
                    pageIndexStream.CopyTo(pageIndexReadStream);

                    indexStream.Seek(0, SeekOrigin.End);
                    vectorStream.Seek(0, SeekOrigin.End);
                    pageIndexStream.Seek(0, SeekOrigin.End);

                    indexReadStream.Position = 0;
                    vectorReadStream.Position = 0;
                    pageIndexReadStream.Position = 0;

                    using (var pageIndexReader = new PageIndexReader(pageIndexReadStream, keepStreamOpen:true))
                    using (var reader = new ColumnReader(pageIndexReader.ReadAll(), indexReadStream, vectorReadStream, _loggerFactory.CreateLogger<ColumnReader>()))
                    {
                        var tree = model.CreateTree(new OptimizedPageIndexingStrategy(model), reader, data);

                        using (var writer = new ColumnWriter(indexStream, keepStreamOpen: true))
                        {
                            writer.CreatePage(tree, vectorStream, new PageIndexWriter(pageStream, keepStreamOpen: true));
                        }
                    }
                }

                pageStream.Position = 0;
                indexStream.Position = 0;
                vectorStream.Position = 0;

                Assert.DoesNotThrow(() =>
                {
                    using (var pageIndexReader = new PageIndexReader(pageStream, keepStreamOpen: true))
                    using (var reader = new ColumnReader(pageIndexReader.ReadAll(), indexStream, vectorStream, _loggerFactory.CreateLogger<ColumnReader>()))
                    {
                        foreach (var word in _data)
                        {
                            foreach (var queryVector in model.CreateEmbedding(word, true))
                            {
                                var hit = reader.ClosestMatchOrNull(queryVector, model);

                                if (hit == null)
                                {
                                    throw new Exception($"unable to find {word} in tree.");
                                }

                                if (hit.Score < model.IdenticalAngle)
                                {
                                    throw new Exception($"unable to score {word}.");
                                }

                                Debug.WriteLine($"{word} matched vector in disk with {hit.Score * 100}% certainty.");
                            }
                        }
                    }
                });
            }
        }

        [Test]
        public void Can_tokenize()
        {
            var model = new BagOfCharsModel();

            foreach (var data in _data)
            {
                var tokens = model.CreateEmbedding(data, true).ToList();
                var labels = tokens.Select(x => x.Label.ToString()).ToList();

                foreach( var token in tokens)
                {
                    Assert.IsTrue(labels.Contains(token.Label));
                }
            }
        }

        [SetUp]
        public void Setup()
        {
            _loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Microsoft", LogLevel.Warning)
                    .AddFilter("System", LogLevel.Warning)
                    .AddDebug();
            });

            _sessionFactory = new SessionFactory(logger: _loggerFactory.CreateLogger<SessionFactory>());
        }

        [TearDown]
        public void TearDown()
        {
            _sessionFactory.Dispose();
        }
    }
}