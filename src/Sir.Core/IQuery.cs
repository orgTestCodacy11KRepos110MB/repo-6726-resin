using System.Collections.Generic;

namespace Sir
{
    public interface IQuery
    {
        IQuery AndQuery { get; set; }
        IQuery NotQuery { get; set; }
        IQuery OrQuery { get; set; }
        HashSet<string> Select { get; }
        IList<ITerm> Terms { get; }
        bool IsUnion { get; set; }
        bool IsIntersection { get; set; }

        IEnumerable<IQuery> All();
        IEnumerable<ITerm> AllTerms();
        int GetCollectionCount();
        void GetNumOfCollections(HashSet<ulong> dic);
        int TotalNumberOfTerms();
    }
}