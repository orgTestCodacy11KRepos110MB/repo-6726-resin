namespace Sir.IO
{
    public class PostingsResolver
    {
        /// <summary>
        /// Read posting list document IDs into memory.
        /// </summary>
        public void Resolve(IQuery query, IStreamDispatcher sessionFactory)
        {
            foreach(var term in query.AllTerms())
            {
                using (var reader = new PostingsReader(term.Directory, sessionFactory))
                    Resolve(term, reader);
            }
        }

        public void Resolve(Term term, PostingsReader postingsReader)
        {
            if (term.PostingsOffsets == null)
                return;

            term.DocumentIds = postingsReader.Read(term.CollectionId, term.KeyId, term.PostingsOffsets);
        }
    }
}