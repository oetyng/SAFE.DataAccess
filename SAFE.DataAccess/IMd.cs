using System.Collections.Generic;

namespace SAFE.DataAccess
{
    public interface IMd
    {
        int Count { get; }
        bool IsFull { get; }
        int Level { get; }
        MdType Type { get; }
        byte[] XORAddress { get; }

        Result<Pointer> Add(Pointer pointer);
        Result<Pointer> Add(string key, Value value);
        Result<Pointer> Delete(string key);
        IEnumerable<(Pointer, Value)> GetAllPointerValues();
        Result<(Pointer, Value)> GetPointerAndValue(string key);
        Result<Value> GetValue(string key);
        IEnumerable<Value> GetValues();
        Result<Pointer> Set(string key, Value value);
    }
}