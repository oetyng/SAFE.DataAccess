using System.Collections.Generic;

namespace SAFE.DataAccess
{
    public interface IIndexer
    {
        void CreateIndex<T>(string[] propertyPath, IEnumerable<(Pointer, Value)> pointerValues);
        void TryIndex(object topLevelObject, Pointer valuePointer);
        void Index(string indexKey, Pointer valuePointer);
        (IEnumerable<T> data, IEnumerable<string> errors) GetAllValues<T>(string indexKey);
        (IEnumerable<(Pointer, Value)> data, IEnumerable<string> errors) GetAllPointersWithValues(string indexKey);
    }
}
