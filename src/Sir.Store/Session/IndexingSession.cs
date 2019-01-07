﻿using Sir.Core;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Sir.Store
{
    /// <summary>
    /// Indexing session targeting a single collection.
    /// </summary>
    public class IndexingSession : CollectionSession, IDisposable
    {
        private readonly IConfigurationProvider _config;
        private readonly ITokenizer _tokenizer;
        private readonly StreamWriter _log;
        private bool _validate;
        private readonly IDictionary<long, VectorNode> _dirty;
        private readonly Stream _vectorStream;
        private bool _flushed;
        //private readonly IDictionary<long, ProducerConsumerQueue<(ulong docId, long keyId, HashSet<string>)>> _tokenWriters;

        public IndexingSession(
            string collectionId, 
            SessionFactory sessionFactory, 
            ITokenizer tokenizer,
            IConfigurationProvider config) : base(collectionId, sessionFactory)
        {
            _config = config;
            _tokenizer = tokenizer;
            _log = Logging.CreateWriter("indexingsession");
            _validate = config.Get("create_index_validation_files") == "true";
            _dirty = new ConcurrentDictionary<long, VectorNode>();
            //_tokenWriters = new Dictionary<long, ProducerConsumerQueue<(ulong docId, long keyId, HashSet<string>)>>();
            _vectorStream = SessionFactory.CreateAsyncAppendStream(
                Path.Combine(SessionFactory.Dir, string.Format("{0}.vec", CollectionId.ToHash())));
        }

        public void Write(IDictionary document)
        {
            try
            {
                var timer = Stopwatch.StartNew();
                var docId = ulong.Parse(document["__docid"].ToString());

                foreach (var column in Analyze(document))
                {
                    var keyId = column.Key;
                    var tokens = column.Value;

                    WriteTokens((docId, keyId, tokens));

                    //GetOrCreateWriter(keyId).Enqueue((docId, keyId, tokens));

                    // validate
                    if (_validate)
                    {
                        var ix = _dirty[keyId];

                        foreach (var token in tokens)
                        {
                            var query = new VectorNode(token);
                            var closestMatch = ix.ClosestMatch(query);

                            if (closestMatch.Score < VectorNode.IdenticalAngle)
                            {
                                throw new DataMisalignedException();
                            }
                        }

                        File.WriteAllText(
                            Path.Combine(SessionFactory.Dir, string.Format("{0}.{1}.{2}.validate", CollectionId.ToHash(), keyId, docId)),
                            string.Join('\n', tokens));
                    }
                }

                _log.Log(string.Format("analyzed doc ID {0} in {1}", document["__docid"], timer.Elapsed));

            }
            catch (Exception ex)
            {
                _log.Log(ex);

                throw;
            }
        }

        public async Task Flush()
        {
            if (_flushed)
                return;

            _vectorStream.Flush();
            _vectorStream.Close();
            _vectorStream.Dispose();

            foreach (var w in _dirty)
            {
                await SerializeColumn(w.Key, w.Value);
            }

            _flushed = true;

            _log.Log(string.Format("***FLUSHED***"));
        }

        private async Task SerializeColumn(long keyId, VectorNode column)
        {
            var postingsWriter = new RemotePostingsWriter(_config);
            var collectionId = CollectionId.ToHash();

            await postingsWriter.Write(collectionId, column);

            var pixFileName = Path.Combine(SessionFactory.Dir, string.Format("{0}.{1}.ixp", collectionId, keyId));

            using (var pageIndexWriter = new PageIndexWriter(SessionFactory.CreateAsyncAppendStream(pixFileName)))
            using (var ixStream = CreateIndexStream(collectionId, keyId))
            {
                var time = Stopwatch.StartNew();

                var page = await column.SerializeTree(ixStream);

                await pageIndexWriter.WriteAsync(page.offset, page.length);

                var size = column.Size();

                _log.Log("serialized column {0} in {1} with size {2},{3} (avg depth {4})",
                    keyId, time.Elapsed, size.depth, size.width, size.avgDepth);
            }
        }

        private IEnumerable<KeyValuePair<long, HashSet<string>>> Analyze(IDictionary doc)
        {
            var docId = (ulong)doc["__docid"];

            foreach (var obj in doc.Keys)
            {
                var key = (string)obj;

                if (key.StartsWith("__"))
                    continue;

                var keyHash = key.ToHash();
                var keyId = SessionFactory.GetKeyId(keyHash);

                var column = new HashSet<string>();
                var val = (IComparable)doc[obj];
                var str = val as string;

                if (str == null || key[0] == '_')
                {
                    var v = val.ToString();

                    if (!string.IsNullOrWhiteSpace(v))
                    {
                        column.Add(v);
                    }
                }
                else
                {
                    var tokens = _tokenizer.Tokenize(str);

                    foreach (var token in tokens)
                    {
                        column.Add(token);
                    }
                }

                yield return new KeyValuePair<long, HashSet<string>>(keyId, column);
            }
        }

        private void WriteTokens((ulong docId, long keyId, HashSet<string> tokens) item)
        {
            var ix = GetOrCreateIndex(item.keyId);

            foreach (var token in item.tokens)
            {
                ix.Add(new VectorNode(token, item.docId), _vectorStream);
            }
        }

        private Stream CreateIndexStream(ulong collectionId, long keyId)
        {
            var fileName = Path.Combine(SessionFactory.Dir, string.Format("{0}.{1}.ix", collectionId, keyId));
            return SessionFactory.CreateAsyncAppendStream(fileName);
        }

        private VectorNode GetOrCreateIndex(long keyId)
        {
            VectorNode root;

            if (!_dirty.TryGetValue(keyId, out root))
            {
                root = new VectorNode();
                _dirty.Add(keyId, root);
            }

            return root;
        }

        public void Dispose()
        {
            if (!_flushed)
            {
                throw new InvalidOperationException();
            }
        }
    }
}