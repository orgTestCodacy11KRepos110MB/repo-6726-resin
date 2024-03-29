﻿using Microsoft.Extensions.Logging;
using Sir.Core;
using Sir.Documents;
using Sir.IO;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir
{
    /// <summary>
    /// Stream dispatcher with helper methods for writing, indexing, optimizing, updating and truncating document collections.
    /// </summary>
    public class SessionFactory : IDisposable, IStreamDispatcher
    {
        private IDictionary<ulong, IDictionary<ulong, long>> _keys;
        private ILogger _logger;
        private readonly object _syncKeys = new object();

        public SessionFactory(ILogger logger = null)
        {
            _logger = logger;
            _keys = new Dictionary<ulong, IDictionary<ulong, long>>();

            LogInformation($"database initiated");
        }

        public IColumnReader CreateColumnReader(string directory, ulong collectionId, long keyId)
        {
            var ixFileName = Path.Combine(directory, string.Format("{0}.{1}.ix", collectionId, keyId));

            if (!File.Exists(ixFileName))
                return new ColumnReader(null, null, null);

            var vectorFileName = Path.Combine(directory, $"{collectionId}.{keyId}.vec");
            var pageIndexFileName = Path.Combine(directory, $"{collectionId}.{keyId}.ixtp");

            using (var pageIndexReader = new PageIndexReader(CreateReadStream(pageIndexFileName)))
            {
                return new ColumnReader(
                    pageIndexReader.ReadAll(),
                    CreateReadStream(ixFileName),
                    CreateReadStream(vectorFileName),
                    _logger);
            }
        }

        public IEnumerable<Document> Select(string directory, ulong collectionId, HashSet<string> select, int skip = 0, int take = 0)
        {
            using (var reader = new DocumentStreamSession(directory, this))
            {
                foreach (var document in reader.ReadDocuments(collectionId, select, skip, take))
                {
                    yield return document;
                }
            }
        }

        public void LogInformation(string message)
        {
            if (_logger != null)
                _logger.LogInformation(message);
        }

        public void LogDebug(string message)
        {
            if (_logger != null)
                _logger.LogDebug(message);
        }

        public void LogError(Exception ex, string message)
        {
            if (_logger != null)
                _logger.LogError(ex, message);
        }

        public long GetDocCount(string directory, string collection)
        {
            var fileName = Path.Combine(directory, $"{collection.ToHash()}.dix");

            if (!File.Exists(fileName))
                return 0;

            return new FileInfo(fileName).Length / DocIndexWriter.BlockSize;
        }

        public void Truncate(string directory, ulong collectionId)
        {
            var count = 0;

            if (Directory.Exists(directory))
            {
                foreach (var file in Directory.GetFiles(directory, $"{collectionId}*"))
                {
                    File.Delete(file);
                    count++;
                }

                lock (_syncKeys)
                {
                    var keyStr = Path.Combine(directory, collectionId.ToString());
                    var key = keyStr.ToHash();
                    _keys.Remove(key, out _);
                }
            }

            LogInformation($"truncated collection {collectionId} ({count} files affected)");
        }

        public void TruncateIndex(string directory, ulong collectionId)
        {
            var count = 0;

            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.ix"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.ixp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.ixtp"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.vec"))
            {
                File.Delete(file);
                count++;
            }
            foreach (var file in Directory.GetFiles(directory, $"{collectionId}*.pos"))
            {
                File.Delete(file);
                count++;
            }

            LogInformation($"truncated index {collectionId} ({count} files affected)");
        }

        public void Rename(string directory, ulong currentCollectionId, ulong newCollectionId)
        {
            var count = 0;

            var from = currentCollectionId.ToString();
            var to = newCollectionId.ToString();

            foreach (var file in Directory.GetFiles(directory, $"{currentCollectionId}*"))
            {
                File.Move(file, file.Replace(from, to));
                count++;
            }

            var key = Path.Combine(directory, currentCollectionId.ToString()).ToHash();

            _keys.Remove(key, out _);

            LogInformation($"renamed collection {currentCollectionId} to {newCollectionId} ({count} files affected)");
        }

        public void Optimize<T>(
            string directory,
            string collection,
            HashSet<string> selectFields, 
            IModel<T> model,
            IIndexReadWriteStrategy indexStrategy,
            int skipDocuments = 0,
            int takeDocuments = 0,
            int reportFrequency = 1000,
            int pageSize = 100000)
        {
            var collectionId = collection.ToHash();

            LogDebug($"optimizing indices for {string.Join(',', selectFields)} in collection {collectionId}");

            using (var debugger = new IndexDebugger(_logger, reportFrequency))
            using (var documents = new DocumentStreamSession(directory, this))
            {
                using (var writeQueue = new ProducerConsumerQueue<IndexSession<T>>(indexSession =>
                {
                    using (var index = new IndexWriter(directory, collectionId, this, logger: _logger))
                    {
                        indexSession.Commit(index);
                    }
                }))
                {
                    var took = 0;
                    var skip = skipDocuments;

                    while (took < takeDocuments)
                    {
                        var payload = documents.ReadDocumentVectors(
                            collectionId,
                            selectFields,
                            model,
                            false,
                            skip,
                            pageSize);

                        var count = 0;

                        using (var indexSession = new IndexSession<T>(model, indexStrategy, this, directory, collectionId))
                        {
                            foreach (var document in payload)
                            {
                                foreach (var node in document.Nodes)
                                {
                                    indexSession.Put(node);
                                }

                                count++;

                                debugger.Step(indexSession);
                            }

                            writeQueue.Enqueue(indexSession);
                        }

                        if (count == 0)
                            break;

                        took += count;
                        skip += pageSize;
                    }
                }
            }

            LogDebug($"optimized collection {collection}");
        }

        public void StoreDataAndBuildInMemoryIndex<T>(IEnumerable<IDocument> job, WriteSession writeSession, IndexSession<T> indexSession, int reportSize = 1000, bool label = true)
        {
            var debugger = new IndexDebugger(_logger, reportSize);

            foreach (var document in job)
            {
                writeSession.Put(document);

                foreach (var field in document.Fields)
                {
                    if (field.Value != null)
                    {
                        indexSession.Put(document.Id, field.KeyId, (T)field.Value, label);
                    }
                }

                debugger.Step(indexSession);
            }
        }

        public void StoreDataAndBuildInMemoryIndex<T>(
            Document document, 
            WriteSession writeSession, 
            IndexSession<T> indexSession, 
            bool label = true)
        {
            writeSession.Put(document);

            foreach (var field in document.Fields)
            {
                if (field.Value != null && field.Value is T typedValue)
                {
                    indexSession.Put(document.Id, field.KeyId, typedValue, label);
                }
            }
        }

        public void BuildIndex<T>(ulong collectionId, IEnumerable<Document> job, IModel<T> model, IIndexReadWriteStrategy indexStrategy, IndexSession<T> indexSession, bool label = true)
        {
            LogDebug($"building index for collection {collectionId}");

            var time = Stopwatch.StartNew();

            using (var queue = new ProducerConsumerQueue<Document>(document =>
            {
                foreach (var field in document.Fields)
                {
                    if (field.Value != null)
                    {
                        indexSession.Put(field.DocumentId, field.KeyId, field.Tokens);
                    }
                }
            }))
            {
                foreach (var document in job)
                {
                    foreach (var field in document.Fields)
                    {
                        if (field.Value != null)
                        {
                            field.Analyze(model, indexStrategy, label, this);
                        }
                    }

                    queue.Enqueue(document);
                }
            }

            LogDebug($"built index (collection {collectionId}) in {time.Elapsed}");
        }

        public void StoreDataAndPersistIndex<T>(string directory, ulong collectionId, IEnumerable<IDocument> job, IModel<T> model, IIndexReadWriteStrategy indexStrategy, int reportSize = 1000)
        {
            using (var writeSession = new WriteSession(new DocumentWriter(directory, collectionId, this)))
            using (var indexSession = new IndexSession<T>(model, indexStrategy, this, directory, collectionId))
            {
                StoreDataAndBuildInMemoryIndex(job, writeSession, indexSession, reportSize);

                using (var stream = new IndexWriter(directory, collectionId, this, logger: _logger))
                {
                    indexSession.Commit(stream);
                }
            }
        }

        public void StoreDataAndPersistIndex<T>(string directory, ulong collectionId, Document document, IModel<T> model, IIndexReadWriteStrategy indexStrategy)
        {
            using (var writeSession = new WriteSession(new DocumentWriter(directory, collectionId, this)))
            using (var indexSession = new IndexSession<T>(model, indexStrategy, this, directory, collectionId))
            {
                StoreDataAndBuildInMemoryIndex(document, writeSession, indexSession);

                using (var stream = new IndexWriter(directory, collectionId, this, logger: _logger))
                {
                    indexSession.Commit(stream);
                }
            }
        }

        public void Store(string directory, ulong collectionId, IEnumerable<Document> job)
        {
            using (var writeSession = new WriteSession(new DocumentWriter(directory, collectionId, this)))
            {
                foreach (var document in job)
                    writeSession.Put(document);
            }
        }

        public void Store(string directory, ulong collectionId, Document document)
        {
            using (var writeSession = new WriteSession(new DocumentWriter(directory, collectionId, this)))
            {
                writeSession.Put(document);
            }
        }

        public void Update(string directory, ulong collectionId, long documentId, long keyId, object value)
        {
            using (var updateSession = new UpdateSession(directory, collectionId, this))
            {
                updateSession.Update(documentId, keyId, value);
            }
        }

        public bool DocumentExists<T>(string directory, string collection, string key, T value, IModel<T> model, bool label = true)
        {
            var query = new QueryParser<T>(directory, this, model, _logger)
                .Parse(collection, value, key, key, and: true, or: false, label);

            if (query != null)
            {
                using (var searchSession = new SearchSession(directory, this, model, new NonOptimizedPageIndexingStrategy(model),  _logger))
                {
                    var document = searchSession.SearchScalar(query);

                    if (document != null)
                    {
                        if (document.Score >= model.IdenticalAngle)
                            return true;
                    }
                }
            }

            return false;
        }

        public bool DocumentExists<T>(string directory, string collection, string key, T value, IModel<T> model, SearchSession searchSession, bool label = true)
        {
            var query = new QueryParser<T>(directory, this, model, _logger)
                .Parse(collection, value, key, key, and: true, or: false, label);

            if (query != null)
            {
                var document = searchSession.SearchScalar(query);

                if (document != null)
                {
                    if (document.Score >= model.IdenticalAngle)
                        return true;
                }
            }

            return false;
        }

        public FileStream CreateLockFile(string directory, ulong collectionId)
        {
            return new FileStream(Path.Combine(directory, collectionId + ".lock"),
                   FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None,
                   4096, FileOptions.RandomAccess | FileOptions.DeleteOnClose);
        }

        private void ReadKeys(string directory)
        {
            foreach (var keyFile in Directory.GetFiles(directory, "*.kmap"))
            {
                var collectionId = ulong.Parse(Path.GetFileNameWithoutExtension(keyFile));
                var key = Path.Combine(directory, collectionId.ToString()).ToHash();

                IDictionary<ulong, long> keys;

                if (!_keys.TryGetValue(key, out keys))
                {
                    keys = new Dictionary<ulong, long>();

                    var timer = Stopwatch.StartNew();

                    using (var stream = new FileStream(keyFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                    {
                        long i = 0;
                        var buf = new byte[sizeof(ulong)];
                        var read = stream.Read(buf, 0, buf.Length);

                        while (read > 0)
                        {
                            keys.Add(BitConverter.ToUInt64(buf, 0), i++);

                            read = stream.Read(buf, 0, buf.Length);
                        }
                    }

                    lock (_syncKeys)
                    {
                        _keys.Add(key, keys);
                    }

                    LogDebug($"loaded key mappings into memory from directory {directory} in {timer.Elapsed}");
                }
            }
        }

        public void RegisterKeyMapping(string directory, ulong collectionId, ulong keyHash, long keyId)
        {
            var key = Path.Combine(directory, collectionId.ToString()).ToHash();

            IDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(key, out keys))
            {
                keys = new ConcurrentDictionary<ulong, long>();

                lock (_syncKeys)
                {
                    _keys.Add(key, keys);
                }
            }

            if (!keys.ContainsKey(keyHash))
            {
                keys.Add(keyHash, keyId);

                using (var stream = CreateAppendStream(directory, collectionId, "kmap"))
                {
                    stream.Write(BitConverter.GetBytes(keyHash), 0, sizeof(ulong));
                }
            }
        }

        public long GetKeyId(string directory, ulong collectionId, ulong keyHash)
        {
            var key = Path.Combine(directory, collectionId.ToString()).ToHash();

            IDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(key, out keys))
            {
                ReadKeys(directory);
            }

            if (keys != null || _keys.TryGetValue(key, out keys))
            {
                return keys[keyHash];
            }

            throw new Exception($"unable to find key {keyHash} for collection {collectionId} in directory {directory}.");
        }

        public bool TryGetKeyId(string directory, ulong collectionId, ulong keyHash, out long keyId)
        {
            var key = Path.Combine(directory, collectionId.ToString()).ToHash();

            IDictionary<ulong, long> keys;

            if (!_keys.TryGetValue(key, out keys))
            {
                ReadKeys(directory);
            }

            if (keys != null || _keys.TryGetValue(key, out keys))
            {
                if (keys.TryGetValue(keyHash, out keyId))
                {
                    return true;
                }
            }

            keyId = -1;
            return false;
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

        public Stream CreateAppendStream(string directory, ulong collectionId, string fileExtension)
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) {}
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateAppendStream(string directory, ulong collectionId, long keyId, string fileExtension)
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{keyId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) {}
            }

            return new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateSeekableWritableStream(string directory, ulong collectionId, long keyId, string fileExtension)
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{keyId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) {}
            }

            return new FileStream(fileName, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
        }

        public Stream CreateSeekableWritableStream(string directory, ulong collectionId, string fileExtension)
        {
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var fileName = Path.Combine(directory, $"{collectionId}.{fileExtension}");

            if (!File.Exists(fileName))
            {
                using (var fs = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) {}
            }

            return new FileStream(fileName, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
        }

        public void Dispose()
        {
            LogDebug($"database disposed");
        }
    }
}