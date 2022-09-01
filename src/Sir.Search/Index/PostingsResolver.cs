namespace Sir.Strings
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

        public void Resolve(ITerm term, PostingsReader postingsReader)
        {
            if (term.PostingsOffset == -1)
                return;

            term.DocumentIds = postingsReader.Read(term.CollectionId, term.KeyId, term.PostingsOffset);
        }
    }
}