using Microsoft.Extensions.Logging;

namespace Sir.IO
{
    public class PostingsResolver
    {
        /// <summary>
        /// Read posting list document IDs into memory.
        /// </summary>
        public void Resolve(IQuery query, IStreamDispatcher sessionFactory, ILogger logger = null)
        {
            foreach(var term in query.AllTerms())
            {
                using (var postingsReader = new PostingsReader(term.Directory, sessionFactory, logger))
                {
                    if (term.PostingsOffsets == null)
                        continue;

                    term.DocumentIds = postingsReader.Read(term.CollectionId, term.KeyId, term.PostingsOffsets);
                }
            }
        }
    }
}