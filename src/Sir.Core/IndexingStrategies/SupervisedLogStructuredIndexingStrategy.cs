﻿using Microsoft.Extensions.Logging;
using Sir.IO;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir
{
    public class SupervisedLogStructuredIndexingStrategy : IIndexReadWriteStrategy
    {
        private readonly IModel _model;

        public SupervisedLogStructuredIndexingStrategy(IModel model)
        {
            _model = model;
        }

        public Hit GetMatchOrNull(ISerializableVector vector, IModel model, ColumnReader reader)
        {
            return reader.ClosestMatchOrNullScanningAllPages(vector, model);
        }

        public void Put<T>(VectorNode column, VectorNode node)
        {
            column.AddOrAppendSupervised(node, _model);
        }

        public void Commit(string directory, ulong collectionId, long keyId, VectorNode tree, IStreamDispatcher streamDispatcher, ILogger logger = null)
        {
            var time = Stopwatch.StartNew();

            using (var vectorStream = streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "vec"))
            using (var postingsStream = streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "pos"))
            using (var columnWriter = new ColumnWriter(streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "ix")))
            using (var pageIndexWriter = new PageIndexWriter(streamDispatcher.CreateAppendStream(directory, collectionId, keyId, "ixtp")))
            {
                var size = columnWriter.CreatePage(tree, vectorStream, postingsStream, pageIndexWriter);

                if (logger != null)
                    logger.LogDebug($"serialized column {keyId}, weight {tree.Weight} {size} in {time.Elapsed}");
            }
        }

        public void Commit(string directory, ulong collectionId, long keyId, SortedList<double, VectorInfo> tree, IStreamDispatcher streamDispatcher, ILogger logger = null)
        {
            throw new System.NotImplementedException();
        }

        public VectorInfoHit GetMatch(ISerializableVector vector, IModel model, ColumnReader reader)
        {
            throw new System.NotImplementedException();
        }
    }
}