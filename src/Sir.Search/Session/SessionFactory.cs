﻿using Microsoft.Extensions.Logging;
using Sir.Core;
using Sir.Documents;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Sir.Search
{
    /// <summary>
    /// Dispatcher of sessions.
    /// </summary>
    public class SessionFactory : IDisposable, ISessionFactory
    {
        private ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> _keys;

        public string Directory { get; }
        public ILogger Logger { get; }

        public SessionFactory(string directory = null, ILogger logger = null)
        {
            var time = Stopwatch.StartNew();

            Directory = directory;

            if (Directory != null && !System.IO.Directory.Exists(Directory))
            {
                System.IO.Directory.CreateDirectory(Directory);
            }

            _keys = LoadKeys();
            Logger = logger;

           LogInformation($"sessionfactory initiated in {time.Elapsed}");
        }

        private void LogInformation(string message)
        {
            if (Logger != null)
                Logger.LogInformation(message);
        }

        private void LogDebug(string message)
        {
            if (Logger != null)
                Logger.LogDebug(message);
        }

        public long GetDocCount(string collection)
        {
            var fileName = Path.Combine(Directory, $"{collection.ToHash()}.dix");

            if (!File.Exists(fileName))
                return 0;

            return new FileInfo(fileName).Length / (sizeof(long) + sizeof(int));
        }

        public void Truncate(ulong collectionId)
        {
            var count = 0;

            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*"))
            {
                File.Delete(file);
                count++;
            }

            _keys.Remove(collectionId, out _);

            LogInformation($"truncated collection {collectionId} ({count} files)");
        }

        public void TruncateIndex(ulong collectionId)
        {
            var count = 0;

            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*.ix"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*.ixp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*.ixtp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*.vec"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in System.IO.Directory.GetFiles(Directory, $"{collectionId}*.pos"))
            {
                File.Delete(file);
                count++;
            }

            LogInformation($"truncated index {collectionId} ({count} files)");
        }

        public void Optimize(
            string collection,
            HashSet<string> selectFields, 
            ITextModel model,
            int skip = 0,
            int take = 0,
            int reportFrequency = 1000,
            int pageSize = 100000,
            bool truncateIndex = true)
        {
            var collectionId = collection.ToHash();
            var debugger = new IndexDebugger(Logger, pageSize);

            if (truncateIndex)
                TruncateIndex(collectionId);

            using (var docStream = new DocumentStreamSession(this))
            using (var documentReader = new DocumentReader(collectionId, this))
            {
                var payload = docStream.ReadDocs(
                        documentReader,
                        selectFields,
                        skip,
                        take);

                using (var writeQueue = new ProducerConsumerQueue<IndexSession<string>>(indexSession =>
                {
                    using (var stream = new WritableIndexStream(collectionId, this, logger: Logger))
                    {
                        stream.Write(indexSession.GetInMemoryIndex());

                        debugger.Step(indexSession, pageSize);
                    }
                }))
                {
                    foreach (var page in payload.Batch(pageSize))
                    {
                        using (var indexSession = new IndexSession<string>(model, model))
                        {
                            using (var indexQueue = new IndexProducerConsumerQueue(field =>
                            {
                                indexSession.Put(field.DocumentId, field.KeyId, field.Tokens);
                            }))
                            {
                                using (var analyzeQueue = new ProducerConsumerQueue<Field>(field =>
                                {
                                    field.Analyze(model);
                                    indexQueue.Enqueue(field);
                                }))
                                {
                                    foreach (var document in page)
                                    {
                                        foreach (var field in document.Fields)
                                        {
                                            analyzeQueue.Enqueue(field);
                                        }
                                    }
                                }
                            }
                            
                            writeQueue.Enqueue(indexSession);
                        }
                    }
                }
            }

            LogInformation($"optimized collection {collection}");
        }

        public void SaveAs(
            ulong targetCollectionId, 
            IEnumerable<Document> documents,
            ITextModel model,
            int reportSize = 1000)
        {
            var job = new TextJob(targetCollectionId, documents, model);

            Write(job, reportSize);
        }

        public void Write(TextJob job, WriteSession writeSession, IndexSession<string> indexSession, int reportSize = 1000)
        {
            LogInformation($"writing to collection {job.CollectionId}");

            var time = Stopwatch.StartNew();
            var debugger = new IndexDebugger(Logger, reportSize);

            foreach (var document in job.Documents)
            {
                writeSession.Put(document);

                //Parallel.ForEach(document, kv =>
                foreach (var field in document.Fields)
                {
                    if (field.Value != null && field.Index)
                    {
                        indexSession.Put(document.Id, field.KeyId, field.Value.ToString());
                    }
                }//);

                debugger.Step(indexSession);
            }

            Logger.LogInformation($"processed write&index job (collection {job.CollectionId}) in {time.Elapsed}");
        }

        public void Write(
            Document document, 
            WriteSession writeSession, 
            IndexSession<string> indexSession)
        {
            writeSession.Put(document);

            foreach (var field in document.Fields)
            {
                if (field.Value != null && field.Index)
                {
                    indexSession.Put(document.Id, field.KeyId, field.Value.ToString());
                }
            }
        }

        public void Index(TextJob job, int reportSize = 1000)
        {
            using (var indexSession = new IndexSession<string>(job.Model, job.Model))
            {
                Index(job, indexSession);

                using (var stream = new WritableIndexStream(job.CollectionId, this, logger: Logger))
                {
                    stream.Write(indexSession.GetInMemoryIndex());
                }
            }
        }

        public void Index<T>(TextJob job, IndexSession<T> indexSession)
        {
            LogInformation($"indexing collection {job.CollectionId}");

            var time = Stopwatch.StartNew();

            using (var queue = new ProducerConsumerQueue<Document>(document =>
            {
                foreach (var field in document.Fields)
                {
                    if (field.Value != null && field.Index)
                    {
                        indexSession.Put(field.DocumentId, field.KeyId, field.Tokens);
                    }
                }
            }))
            {
                foreach (var document in job.Documents)
                {
                    foreach (var field in document.Fields)
                    {
                        if (field.Value != null && field.Index)
                        {
                            field.Analyze(job.Model);
                        }
                    }

                    queue.Enqueue(document);
                }
            }

            LogInformation($"processed indexing job (collection {job.CollectionId}) in {time.Elapsed}");
        }

        public void Write(TextJob job, int reportSize = 1000)
        {
            using (var writeSession = new WriteSession(new DocumentWriter(job.CollectionId, this)))
            using (var indexSession = new IndexSession<string>(job.Model, job.Model))
            {
                Write(job, writeSession, indexSession, reportSize);

                using (var stream = new WritableIndexStream(job.CollectionId, this, logger: Logger))
                {
                    stream.Write(indexSession.GetInMemoryIndex());
                }
            }
        }

        public void Write(
            ulong collectionId,
            IEnumerable<Document> documents, 
            ITextModel model, 
            int reportSize = 1000
            )
        {
            using (var writeSession = new WriteSession(new DocumentWriter(collectionId, this)))
            using (var indexSession = new IndexSession<string>(model, model))
            {
                Write(
                    new TextJob(
                        collectionId,
                        documents,
                        model),
                    writeSession,
                    indexSession,
                    reportSize);

                using (var stream = new WritableIndexStream(collectionId, this, logger: Logger))
                {
                    stream.Write(indexSession.GetInMemoryIndex());
                }
            }
        }

        public FileStream CreateLockFile(ulong collectionId)
        {
            return new FileStream(Path.Combine(Directory, collectionId + ".lock"),
                   FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None,
                   4096, FileOptions.RandomAccess | FileOptions.DeleteOnClose);
        }

        public void RefreshKeys()
        {
            _keys = LoadKeys();
        }

        public ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>> LoadKeys()
        {
            var timer = Stopwatch.StartNew();
            var allkeys = new ConcurrentDictionary<ulong, ConcurrentDictionary<ulong, long>>();

            if (Directory == null)
            {
                return allkeys;
            }

            foreach (var keyFile in System.IO.Directory.GetFiles(Directory, "*.kmap"))
            {
                var collectionId = ulong.Parse(Path.GetFileNameWithoutExtension(keyFile));
                ConcurrentDictionary<ulong, long> keys;

                if (!allkeys.TryGetValue(collectionId, out keys))
                {
                    keys = new ConcurrentDictionary<ulong, long>();
                    allkeys.GetOrAdd(collectionId, keys);
                }

                using (var stream = new FileStream(keyFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                {
                    long i = 0;
                    var buf = new byte[sizeof(ulong)];
                    var read = stream.Read(buf, 0, buf.Length);

                    while (read > 0)
                    {
                        keys.GetOrAdd(BitConverter.ToUInt64(buf, 0), i++);

                        read = stream.Read(buf, 0, buf.Length);
                    }
                }
            }

            LogInformation($"loaded keyHash -> keyId mappings into memory for {allkeys.Count} collections in {timer.Elapsed}");

            return allkeys;
        }

        public void RegisterKeyMapping(ulong collectionId, ulong keyHash, long keyId)
        {
            ConcurrentDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(collectionId, out keys))
            {
                keys = new ConcurrentDictionary<ulong, long>();
                _keys.GetOrAdd(collectionId, keys);
            }

            if (!keys.ContainsKey(keyHash))
            {
                keys.GetOrAdd(keyHash, keyId);

                using (var stream = CreateAppendStream(collectionId, "kmap"))
                {
                    stream.Write(BitConverter.GetBytes(keyHash), 0, sizeof(ulong));
                }
            }
        }

        public long GetKeyId(ulong collectionId, ulong keyHash)
        {
            return _keys[collectionId][keyHash];
        }

        public bool TryGetKeyId(ulong collectionId, ulong keyHash, out long keyId)
        {
            var keys = _keys.GetOrAdd(collectionId, new ConcurrentDictionary<ulong, long>());

            if (!keys.TryGetValue(keyHash, out keyId))
            {
                keyId = -1;
                return false;
            }

            return true;
        }

        public ISearchSession CreateSearchSession(IModel model)
        {
            return new SearchSession(
                this,
                model,
                new PostingsReader(this),
                Logger);
        }

        public Stream CreateAsyncReadStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.Asynchronous);
        }

        public Stream CreateReadStream(string fileName)
        {
            LogDebug($"opened {fileName}");

            return new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        }

        public Stream CreateAsyncAppendStream(string fileName)
        {
            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite, 4096, FileOptions.Asynchronous);
        }

        public Stream CreateAppendStream(ulong collectionId, string fileExtension)
        {
            var fileName = Path.Combine(Directory, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                {
                }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateAppendStream(ulong collectionId, long keyId, string fileExtension)
        {
            var fileName = Path.Combine(Directory, $"{collectionId}.{keyId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                {
                }
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public bool CollectionExists(ulong collectionId)
        {
            return File.Exists(Path.Combine(Directory, collectionId + ".vec"));
        }

        public bool CollectionIsIndexOnly(ulong collectionId)
        {
            if (!CollectionExists(collectionId))
                throw new InvalidOperationException($"{collectionId} dows not exist");

            return !File.Exists(Path.Combine(Directory, collectionId + ".docs"));
        }

        public void Dispose()
        {
        }
    }

    public class IndexProducerConsumerQueue : IDisposable
    {
        private readonly ConcurrentDictionary<long, ProducerConsumerQueue<Field>> _queues;
        private readonly int _numOfConsumers;
        private readonly Action<Field> _consumingAction;

        public IndexProducerConsumerQueue(Action<Field> consumingAction, int numOfConsumers = 1)
        {
            if (consumingAction == null)
            {
                throw new ArgumentNullException(nameof(consumingAction));
            }

            _numOfConsumers = numOfConsumers;
            _consumingAction = consumingAction;
            _queues = new ConcurrentDictionary<long, ProducerConsumerQueue<Field>>();
        }

        public void Enqueue(Field field)
        {
            var queue = _queues.GetOrAdd(field.KeyId, new ProducerConsumerQueue<Field>(_consumingAction, _numOfConsumers));
            
            queue.Enqueue(field);
        }

        public void Dispose()
        {
            foreach (var queue in _queues.Values)
            {
                queue.Dispose();
            }
        }
    }
}