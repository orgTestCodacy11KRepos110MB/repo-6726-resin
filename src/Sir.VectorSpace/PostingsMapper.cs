namespace Sir.VectorSpace
{
    public static class PostingsMapper
    {
        /// <summary>
        /// Read document IDs into memory.
        /// </summary>
        public static void Map(Query query, IStreamDispatcher sessionFactory)
        {
            foreach(var term in query.AllTerms())
            {
                using (var reader = new PostingsReader(term.Directory, sessionFactory))
                    Map(term, reader);
            }
        }

        public static void Map(ITerm term, PostingsReader postingsReader)
        {
            if (term.PostingsOffsets == null)
                return;

            term.Result = postingsReader.Read(term.CollectionId, term.KeyId, term.PostingsOffsets);
        }
    }
}