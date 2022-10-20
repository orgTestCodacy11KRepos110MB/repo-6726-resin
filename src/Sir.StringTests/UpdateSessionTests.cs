using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Sir.Documents;
using Sir.Strings;
using System;
using System.Diagnostics;
using System.Linq;

namespace Sir.Tests
{
    public class UpdateSessionTests
    {
        private ILoggerFactory _loggerFactory;
        private SessionFactory _sessionFactory;
        private string _directory = @"c:\temp\sir_tests";

        private readonly string[] _data = new string[] { "apple", "apples", "apricote", "apricots", "avocado", "avocados", "banana", "bananas", "blueberry", "blueberries", "cantalope" };

        [Test]
        public void Can_update_string_field()
        {
            var model = new BagOfCharsModel();
            const string collection = "Can_update_string_field";
            var collectionId = collection.ToHash();
            const string fieldName = "description";
            const string updatedWord = "xylophone";

            for (int documentIdToUpdate = 0; documentIdToUpdate < _data.Length; documentIdToUpdate++)
            {
                _sessionFactory.Truncate(_directory, collectionId);

                using (var index = new IndexWriter(_directory, collectionId, _sessionFactory))
                using (var writeSession = new WriteSession(new DocumentWriter(_directory, collectionId, _sessionFactory)))
                {
                    var keyId = writeSession.EnsureKeyExists(fieldName);

                    for (long i = 0; i < _data.Length; i++)
                    {
                        var data = _data[i];

                        using (var indexSession = new InMemoryIndexSession<string>(model, new NonOptimizedPageIndexingStrategy(model), _sessionFactory, _directory, collectionId))
                        {
                            var doc = new Document(new Field[] { new Field(fieldName, data) });

                            writeSession.Put(doc);
                            indexSession.Put(doc.Id, keyId, data, true);
                            indexSession.Commit(index);
                        }
                    }
                }

                var queryParser = new QueryParser<string>(_directory, _sessionFactory, model);

                using (var searchSession = new SearchSession(_directory, _sessionFactory, model, new NonOptimizedPageIndexingStrategy(model), _loggerFactory.CreateLogger<SearchSession>()))
                {
                    Assert.DoesNotThrow(() =>
                    {
                        foreach (var word in _data)
                        {
                            var query = queryParser.Parse(collection, word, fieldName, fieldName, and: true, or: false, label: true);
                            var result = searchSession.Search(query, 0, 1);
                            var document = result.Documents.FirstOrDefault();

                            if (document == null)
                            {
                                throw new Exception($"unable to find {word}.");
                            }

                            if (document.Score < model.IdenticalAngle)
                            {
                                throw new Exception($"unable to score {word}.");
                            }

                            Debug.WriteLine($"{word} matched with {document.Score * 100}% certainty.");
                        }
                    });
                }

                using (var updateSession = new UpdateSession(_directory, collectionId, _sessionFactory))
                {
                    updateSession.Update(documentIdToUpdate, 0, updatedWord);
                }

                using (var searchSession = new SearchSession(_directory, _sessionFactory, model, new NonOptimizedPageIndexingStrategy(model), _loggerFactory.CreateLogger<SearchSession>()))
                {
                    Assert.DoesNotThrow(() =>
                    {
                        var count = 0;

                        foreach (var word in _data)
                        {
                            if (count++ == documentIdToUpdate)
                            {
                                continue;
                            }

                            var query = queryParser.Parse(collection, word, fieldName, fieldName, and: true, or: false, label:true);
                            var result = searchSession.Search(query, 0, 1);
                            var document = result.Documents.FirstOrDefault();

                            if (document == null)
                            {
                                throw new Exception($"unable to find {word}.");
                            }

                            if (document.Score < model.IdenticalAngle)
                            {
                                throw new Exception($"unable to score {word}.");
                            }

                            Debug.WriteLine($"{word} matched with {document.Score * 100}% certainty.");
                        }
                    });
                    var r = searchSession.Search(queryParser.Parse(collection, _data[documentIdToUpdate], fieldName, fieldName, and: true, or: false, label: true), 0, 1);
                    Assert.IsTrue(updatedWord == r.Documents.First().Fields.First().Value.ToString());
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