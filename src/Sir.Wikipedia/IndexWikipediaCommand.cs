using Microsoft.Extensions.Logging;
using Sir.Core;
using Sir.Documents;
using Sir.IO;
using Sir.Strings;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;

namespace Sir.Wikipedia
{
    /// <summary>
    /// Download JSON search index dump here: 
    /// https://dumps.wikimedia.org/other/cirrussearch/current/enwiki-20201026-cirrussearch-content.json.gz
    /// </summary>
    /// <example>
    /// indexwikipedia --directory C:\projects\resin\src\Sir.HttpServer\AppData\database --file d:\enwiki-20211122-cirrussearch-content.json.gz --collection wikipedia --skip 0 --take 1000
    /// </example>
    public class IndexWikipediaCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            // Required
            var dataDirectory = args["directory"];
            var fileName = args["file"];
            var collection = args["collection"];

            // Optional
            var skip = args.ContainsKey("skip") ? int.Parse(args["skip"]) : 0;
            var take = args.ContainsKey("take") ? int.Parse(args["take"]) : int.MaxValue;
            var sampleSize = args.ContainsKey("sampleSize") ? int.Parse(args["sampleSize"]) : 1000;
            var pageSize = args.ContainsKey("pageSize") ? int.Parse(args["pageSize"]) : 100000;

            var collectionId = collection.ToHash();
            var fieldsOfInterest = new HashSet<string> { "title", "text", "url" };

            if (take == 0)
                take = int.MaxValue;

            if (!File.Exists(fileName))
            {
                throw new FileNotFoundException($"This file could not be found: {fileName}. Download a wikipedia JSON dump here:  https://dumps.wikimedia.org/other/cirrussearch/current/");
            }

            var model = new BagOfCharsModel();
            var indexStrategy = new LogStructuredIndexingStrategy(model);
            var payload = WikipediaHelper.Read(fileName, skip, take, fieldsOfInterest).Batch(pageSize);

            using (var streamDispatcher = new SessionFactory(logger))
            {
                using (var debugger = new IndexDebugger(logger, sampleSize))
                {
                    foreach (var page in payload)
                    {
                        using (var writeSession = new WriteSession(new DocumentWriter(streamDispatcher, dataDirectory, collectionId)))
                        using (var indexSession = new IndexSession<string>(model, indexStrategy, streamDispatcher, dataDirectory, collectionId, logger))
                        {
                            foreach (var document in page)
                            {
                                writeSession.Put(document);

                                foreach (var field in document.Fields)
                                {
                                    indexSession.Put(document.Id, field.KeyId, (string)field.Value, label: false);
                                }

                                debugger.Step(indexSession);
                            }

                            indexSession.Commit();
                        }
                    }
                }
            }
        }

        private static void Print(string name, VectorNode tree)
        {
            var diagram = PathFinder.Visualize(tree);
            File.WriteAllText($@"c:\temp\{name}.txt", diagram);
        }
    }
}