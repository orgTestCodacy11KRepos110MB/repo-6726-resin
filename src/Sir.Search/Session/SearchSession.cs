using Microsoft.Extensions.Logging;
using Sir.VectorSpace;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir.Search
{
    /// <summary>
    /// Read session targeting multiple collections.
    /// </summary>
    public class SearchSession : DocumentStreamSession, IDisposable, ISearchSession
    {
        private readonly Database _sessionFactory;
        private readonly IModel _model;
        private readonly PostingsResolver _postingsResolver;
        private readonly Scorer _scorer;
        private readonly ILogger _logger;

        public SearchSession(
            string directory, 
            Database sessionFactory,
            IModel model,
            ILogger logger = null,
            PostingsResolver postingsResolver = null,
            Scorer scorer = null) : base(directory, sessionFactory)
        {
            _sessionFactory = sessionFactory;
            _model = model;
            _postingsResolver = postingsResolver ?? new PostingsResolver();
            _scorer = scorer ?? new Scorer();
            _logger = logger;
        }

        public SearchResult Search(Query query, int skip, int take)
        {
            var result = Execute(query, skip, take);

            if (result != null)
            {
                var numOfTerms = query.TotalNumberOfTerms();
                var scoreMultiplier = (double)1 / numOfTerms;
                var docs = ReadDocs(result.SortedDocuments, query.Select, scoreMultiplier);

                return new SearchResult(query, result.Total, docs.Count, docs);
            }

            return new SearchResult(query, 0, 0, new Document[0]);
        }

        public Document SearchScalar(Query query)
        {
            var result = Execute(query, 0, 1);

            if (result != null)
            {
                var numOfTerms = query.TotalNumberOfTerms();
                var scoreMultiplier = (double)1 / numOfTerms;
                var docs = ReadDocs(result.SortedDocuments, query.Select, scoreMultiplier);

                return docs.Count > 0 ? docs[0] : null;
            }

            return null;
        }

        private ScoredResult Execute(Query query, int skip, int take)
        {
            var timer = Stopwatch.StartNew();

            // Scan
            Scan(query);
            LogDebug($"scanning took {timer.Elapsed}");
            timer.Restart();

            // Read postings lists
            _postingsResolver.Resolve(query, _sessionFactory);
            LogDebug($"reading postings took {timer.Elapsed}");
            timer.Restart();
            
            // Score
            IDictionary<(ulong CollectionId, long DocumentId), double> scoredResult = new Dictionary<(ulong, long), double>();
            _scorer.Score(query, ref scoredResult);
            LogDebug($"scoring took {timer.Elapsed}");
            timer.Restart();

            // Sort
            var sorted = Sort(scoredResult, skip, take);
            LogDebug($"sorting took {timer.Elapsed}");

            return sorted;
        }

        /// <summary>
        /// Scan the index to find the query terms closest matching nodes and record their posting list addresses.
        /// </summary>
        private void Scan(Query query)
        {
            if (query == null)
                return;

            var readers = new Dictionary<(string, ulong, long), IColumnReader>();

            try
            {
                foreach (var term in query.AllTerms())
                {
                    IColumnReader reader;
                    var key = (term.Directory, term.CollectionId, term.KeyId);

                    if (!readers.TryGetValue(key, out reader))
                    {
                        reader = GetColumnReader(term.Directory, term.CollectionId, term.KeyId);

                        if (reader != null)
                        {
                            readers.Add(key, reader);
                        }
                    }

                    if (reader != null)
                    {
                        var hit = reader.ClosestMatch(term.Vector, _model);

                        if (hit != null)
                        {
                            term.Score = hit.Score;
                            term.PostingsOffsets = hit.Node.PostingsOffsets ?? new List<long> { hit.Node.PostingsOffset };
                        }
                    }
                }
            }
            finally
            {
                foreach (var reader in readers.Values)
                {
                    reader.Dispose();
                }
            }
        }

        private static ScoredResult Sort(
            IDictionary<(ulong CollectionId, long DocumentId), double> documents,
            int skip, 
            int take)
        {
            var sortedByScore = new List<KeyValuePair<(ulong, long), double>>(documents);

            sortedByScore.Sort(
                delegate (KeyValuePair<(ulong, long), double> pair1,
                KeyValuePair<(ulong, long), double> pair2)
                {
                    return pair2.Value.CompareTo(pair1.Value);
                }
            );

            var index = skip > 0 ? skip : 0;
            int count;

            if (take == 0)
                count = sortedByScore.Count - (index);
            else
                count = Math.Min(sortedByScore.Count - (index), take);

            return new ScoredResult 
            { 
                SortedDocuments = sortedByScore.GetRange(index, count), 
                Total = sortedByScore.Count 
            };
        }

        /// <summary>
        /// https://stackoverflow.com/questions/3719719/fastest-safe-sorting-algorithm-implementation
        /// </summary>
        private static void QuickSort(int[] data, int left, int right)
        {
            int i = left - 1,
                j = right;

            while (true)
            {
                int d = data[left];
                do i++; while (data[i] < d);
                do j--; while (data[j] > d);

                if (i < j)
                {
                    int tmp = data[i];
                    data[i] = data[j];
                    data[j] = tmp;
                }
                else
                {
                    if (left < j) QuickSort(data, left, j);
                    if (++j < right) QuickSort(data, j, right);
                    return;
                }
            }
        }

        private IList<Document> ReadDocs(
            IEnumerable<KeyValuePair<(ulong collectionId, long docId), double>> docIds, 
            HashSet<string> select,
            double scoreMultiplier = 1)
        {
            var result = new List<Document>();
            var timer = Stopwatch.StartNew();

            foreach (var d in docIds)
            {
                var doc = ReadDocument(d.Key, select, d.Value * scoreMultiplier);

                if (doc != null)
                    result.Add(doc);
            }

            LogDebug($"reading documents took {timer.Elapsed}");

            return result;
        }

        private IColumnReader GetColumnReader(string directory, ulong collectionId, long keyId)
        {
            var ixFileName = Path.Combine(directory, string.Format("{0}.{1}.ix", collectionId, keyId));

            if (!File.Exists(ixFileName))
                return null;

            var vectorFileName = Path.Combine(directory, $"{collectionId}.{keyId}.vec");
            var pageIndexFileName = Path.Combine(directory, $"{collectionId}.{keyId}.ixtp");

            using (var pageIndexReader = new PageIndexReader(_sessionFactory.CreateReadStream(pageIndexFileName)))
            {
                return new ColumnReader(
                    pageIndexReader.ReadAll(),
                    _sessionFactory.CreateReadStream(ixFileName),
                    _sessionFactory.CreateReadStream(vectorFileName),
                    _sessionFactory,
                    _logger);
            }
        }

        private void LogInformation(string message)
        {
            if (_logger != null)
                _logger.LogInformation(message);
        }

        private void LogDebug(string message)
        {
            if (_logger != null)
                _logger.LogDebug(message);
        }

        private void LogError(Exception ex, string message)
        {
            if (_logger != null)
                _logger.LogError(ex, message);
        }

        public override void Dispose()
        {
            base.Dispose();
        }
    }
}