
namespace SAFE.DataAccess
{
    public class Result<T>
    {
        public bool HasValue { get; private set; }
        public string ErrorMsg { get; }
        public int? ErrorCode { get; }

        public T Value { get; private set; }

        internal Result(T value, bool hasValue, int? errorCode = null, string errorMsg = "")
        {
            Value = value;
            HasValue = hasValue;
            ErrorCode = errorCode;
            ErrorMsg = errorMsg ?? string.Empty;
        }
    }

    public static class Result
    {
        public static Result<T> OK<T>(T value)
        {
            return new Result<T>(value, true);
        }

        public static Result<T> Fail<T>(int errorCode, string errorMsg)
        {
            return new Result<T>(default(T), false, errorCode, errorMsg);
        }
    }

    public class KeyNotFound<T> : Result<T>
    {
        public KeyNotFound(string info = null)
            : base(default(T), false, ErrorCodes.KEY_NOT_FOUND, $"Key not found! {info}")
        { }
    }

    public class ValueDeleted<T> : Result<T>
    {
        public ValueDeleted(string info = null)
            : base(default(T), false, ErrorCodes.VALUE_DELETED, $"Value deleted! {info}")
        { }
    }

    public class ValueAlreadyExists<T> : Result<T>
    {
        public ValueAlreadyExists(string info = null)
            : base(default(T), false, ErrorCodes.VALUE_ALREADY_EXISTS, $"Value already exists! {info}")
        { }
    }

    public class VersionMismatch<T> : Result<T>
    {
        public VersionMismatch(string info = null)
            : base(default(T), false, ErrorCodes.VERSION_EXCEPTION, $"Version mismatch! {info}")
        { }
    }

    public class DeserializationError<T> : Result<T>
    {
        public DeserializationError(string info = null)
            : base(default(T), false, ErrorCodes.DESERIALIZATION_ERROR, $"Deserialization error! {info}")
        { }
    }

    public class MdOutOfEntriesError<T> : Result<T>
    {
        public MdOutOfEntriesError(string info = null)
            : base(default(T), false, ErrorCodes.MD_OUT_OF_ENTRIES, $"Md has no more entries! {info}")
        { }
    }

    public class ArgumentOutOfRange<T> : Result<T>
    {
        public ArgumentOutOfRange(string info = null)
            : base(default(T), false, ErrorCodes.ARGUMENT_OUT_OF_RANGE, $"Argument out of range! {info}")
        { }
    }

    public class MultipleResults<T> : Result<T>
    {
        public MultipleResults(string info = null)
            : base(default(T), false, ErrorCodes.MULTIPLE_RESULTS, $"Multiple results! {info}")
        { }
    }

    public class InvalidOperation<T> : Result<T>
    {
        public InvalidOperation(string info = null)
            : base(default(T), false, ErrorCodes.MULTIPLE_RESULTS, $"Invalid operation! {info}")
        { }
    }

    //public class DataNotFound<T> : Result<T>
    //{
    //    public DataNotFound(string info = null)
    //        : base(default(T), false, ErrorCodes.DATA_NOT_FOUND, $"Data not found! {info}")
    //    { }
    //}
}
