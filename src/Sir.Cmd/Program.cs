﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.Documents;
using Sir.IO;
using Sir.Strings;

namespace Sir.Cmd
{
    class Program
    {
        static void Main(string[] args)
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Microsoft", LogLevel.Warning)
                    .AddFilter("System", LogLevel.Warning)
                    .AddFilter("Sir", LogLevel.Debug)
                    .AddConsole()
                    .AddEventLog(new Microsoft.Extensions.Logging.EventLog.EventLogSettings 
                    { 
                        SourceName = "Sir.Cmd", 
                        Filter = (source, level) => level >= LogLevel.Debug
                    });
            });

            var logger = loggerFactory.CreateLogger("Sir");

            logger.LogDebug($"processing command: {string.Join(" ", args)}");

            var command = args[0].ToLower();
            var flags = ParseArgs(args);
            var plugin = ResolvePlugin(command);
            var time = Stopwatch.StartNew();

            if (plugin != null)
            {
                try
                {
                    plugin.Run(flags, logger);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, ex.Message);
                }
            }
            else if ((command == "slice"))
            {
                Slice(flags);
            }
            else if (command == "truncate")
            {
                Truncate(flags["directory"], flags["collection"], logger);
            }
            else if (command == "truncate-index")
            {
                TruncateIndex(flags["directory"], flags["collection"], logger);
            }
            else if (command == "optimize")
            {
                Optimize(flags, logger);
            }
            else if (command == "rename")
            {
                Rename(flags["directory"], flags["collection"], flags["newCollection"], logger);
            }
            else
            {
                logger.LogDebug("unknown command: {0}", command);

                return;
            }

            logger.LogDebug($"executed {command} in {time.Elapsed}");
        }

        private static ICommand ResolvePlugin(string command)
        {
            var reader = new PluginReader(Directory.GetCurrentDirectory());
            var plugins = reader.Read<ICommand>("command");

            if (!plugins.ContainsKey(command))
                return null;

            return plugins[command];
        }

        private static IDictionary<string, string> ParseArgs(string[] args)
        {
            var dic = new Dictionary<string, string>();

            for (int i = 1; i < args.Length; i += 2)
            {
                var key = args[i].Replace("--", "");
                var value = args[i + 1];

                if (value.StartsWith("--"))
                {
                    dic.Add(key, "true");
                    i--;
                }
                else
                {
                    dic.Add(key, i == args.Length - 1 ? null : value);
                }
            }

            return dic;
        }

        private static void Optimize(IDictionary<string, string> args, ILogger logger)
        {
            var dataDirectory = args["directory"];
            var collection = args["collection"];
            var skip = int.Parse(args["skip"]);
            var take = int.Parse(args["take"]);
            var reportFrequency = int.Parse(args["reportFrequency"]);
            var pageSize = int.Parse(args["pageSize"]);
            var fields = new HashSet<string>(args["fields"].Split(','));
            var model = new BagOfCharsModel();

            using (var sessionFactory = new SessionFactory(logger))
            {
                sessionFactory.Optimize(
                    dataDirectory,
                    collection, 
                    fields,
                    model,
                    new NonOptimizedPageIndexingStrategy(model),
                    skip,
                    take,
                    reportFrequency,
                    pageSize);
            }
        }

        private static void Slice(IDictionary<string, string> args)
        {
            var file = args["sourceFileName"];
            var slice = args["resultFileName"];
            var len = int.Parse(args["length"]);

            Span<byte> buf = new byte[len];

            using (var fs = File.OpenRead(file))
            using (var target = File.Create(slice))
            {
                fs.Read(buf);
                target.Write(buf);
            }
        }

        private static void Truncate(string dataDirectory, string collection, ILogger log)
        {
            var collectionId = collection.ToHash();

            using (var sessionFactory = new SessionFactory(log))
            {
                sessionFactory.Truncate(dataDirectory, collectionId);
            }
        }

        private static void TruncateIndex(string dataDirectory, string collection, ILogger log)
        {
            var collectionId = collection.ToHash();

            using (var sessionFactory = new SessionFactory(log))
            {
                sessionFactory.TruncateIndex(dataDirectory, collectionId);
            }
        }

        private static void Rename(string dataDirectory, string currentCollectionName, string newCollectionName, ILogger log)
        {
            using (var sessionFactory = new SessionFactory(log))
            {
                sessionFactory.Rename(dataDirectory, currentCollectionName.ToHash(), newCollectionName.ToHash());
            }
        }

        private static void Serialize(IEnumerable<object> docs, Stream stream)
        {
            using (StreamWriter writer = new StreamWriter(stream))
            using (JsonTextWriter jsonWriter = new JsonTextWriter(writer))
            {
                JsonSerializer ser = new JsonSerializer();
                ser.Serialize(jsonWriter, docs);
                jsonWriter.Flush();
            }
        }
    }

    public class AnalyzeDocumentCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dataDirectory = args["directory"];
            var collection = args["collection"];
            var documentId = long.Parse(args["documentId"]);
            var select = new HashSet<string>(args["select"].Split(new char[] { ',', ' ' }, StringSplitOptions.RemoveEmptyEntries));
            var collectionId = collection.ToHash();
            var model = new BagOfCharsModel();

            using (var sessionFactory = new SessionFactory(logger))
            using (var documents = new DocumentStreamSession(dataDirectory, sessionFactory))
            using (var documentReader = new DocumentReader(dataDirectory, collectionId, sessionFactory))
            {
                var doc = documents.ReadDocument((collectionId, documentId), select, documentReader);

                foreach (var field in doc.Fields)
                {
                    var tokens = model.CreateEmbedding(field.Value.ToString(), true);
                    var tree = new VectorNode();

                    foreach (var token in tokens)
                    {
                        tree.AddOrAppend(new VectorNode(token), model);
                    }

                    Console.WriteLine(field.Name);
                    Console.WriteLine(PathFinder.Visualize(tree));
                    Console.WriteLine(string.Join('\n', tokens));
                }
            }
        }
    }
}