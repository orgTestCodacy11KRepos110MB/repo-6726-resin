using Microsoft.Extensions.Logging;
using Sir.Search;
using Sir.Wikipedia;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir.Cmd
{
    public class BenchmarkCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            if (args.ContainsKey("tokenize"))
                RunTokenizeBenchmark(args, logger);
            else if (args.ContainsKey("index"))
                RunIndexBenchmark(args, logger);
        }

        public void RunTokenizeBenchmark(IDictionary<string, string> args, ILogger logger)
        {
            const int numOfDocs = 10000;
            const int numOfRuns = 10;
            var fileName = args["file"];
            var model = new BagOfCharsModel();
            var documents = new List<Document>(WikipediaHelper.Read(fileName, 0, numOfDocs, new HashSet<string> { "text" }));
            var timer = Stopwatch.StartNew();

            for (int i = 0; i < numOfRuns; i++)
            {
                foreach (var document in documents)
                {
                    model.CreateEmbedding((string)document.Fields[0].Value, false);
                }
            }

            var totalTime = timer.Elapsed;
            var avgPerRun = totalTime.TotalMilliseconds / numOfRuns;
            var avgPerDoc = totalTime.TotalMilliseconds / documents.Count;
            Console.WriteLine($"tokenized {documents.Count * numOfRuns} documents in a total of {timer.Elapsed}. Average {avgPerRun} ms/run. Average {avgPerDoc} ms/document.");
        }

        public void RunIndexBenchmark(IDictionary<string, string> args, ILogger logger)
        {
            const int numOfRuns = 1;

            var timer = Stopwatch.StartNew();

            for (int i = 0; i < numOfRuns; i++)
            {
                new Database(logger).Truncate(args["directory"], "wikipedia".ToHash());
                new IndexWikipediaCommand().Run(args, logger);
            }

            var numOfDocs = int.Parse(args["take"]);
            var totalTime = timer.Elapsed;
            var avgPerRun = totalTime.TotalMilliseconds / numOfRuns;
            var avgPerDoc = totalTime.TotalMilliseconds / numOfDocs;
            Console.WriteLine($"indexed {numOfDocs * numOfRuns} documents in a total of {timer.Elapsed}. Average {avgPerRun} ms/run. Average {avgPerDoc} ms/document.");
        }
    }
}