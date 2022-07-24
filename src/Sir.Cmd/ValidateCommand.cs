using Microsoft.Extensions.Logging;
using Sir.Search;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir.Cmd
{
    public class ValidateCommand : ICommand
    {
        /// <summary>
        /// E.g. validate --directory C:\projects\resin\src\Sir.HttpServer\AppData\database --collection wikipedia --skip 0 --take 1000
        /// </summary>
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dir = args["directory"];
            var collection = args["collection"];
            var skip = int.Parse(args["skip"]);
            var take = int.Parse(args["take"]);
            var collectionId = collection.ToHash();
            var model = new BagOfCharsModel();
            var selectFields = new HashSet<string> { "title" };
            var time = Stopwatch.StartNew();

            using (var sessionFactory = new SessionFactory(logger))
            {
                using (var validateSession = new ValidateSession<string>(
                    collectionId, 
                    new SearchSession(dir, sessionFactory, model, logger), 
                    new QueryParser<string>(dir, sessionFactory, model, logger)))
                {
                    using (var documents = new DocumentStreamSession(dir, sessionFactory))
                    {
                        foreach (var doc in documents.ReadDocuments(collectionId, selectFields, skip, take))
                        {
                            validateSession.Validate(doc);

                            Console.WriteLine(doc.Id);
                        }
                    }
                }
            }

            Console.WriteLine("validate took {0}", time.Elapsed);
        }
    }
}