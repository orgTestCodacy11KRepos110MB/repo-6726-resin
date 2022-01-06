namespace Sir.VectorSpace
{
    public static class PostingsMaterializer
    {
        /// <summary>
        /// Read document IDs into memory.
        /// </summary>
        public static void Materialize(Query query, IStreamDispatcher sessionFactory)
        {
            foreach(var term in query.AllTerms())
            {
                using (var reader = new PostingsReader(term.Directory, sessionFactory))
                    Materialize(term, reader);
            }
        }

        public static void Materialize(ITerm term, PostingsReader postingsReader)
        {
            if (term.PostingsOffsets == null)
                return;

            term.Result = postingsReader.Read(term.CollectionId, term.KeyId, term.PostingsOffsets);
        }
    }
}