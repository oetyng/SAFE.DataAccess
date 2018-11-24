using System;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    public class MdAccess
    {
        static Func<MdLocator, Task<Result<IMd>>> _locator;
        static Func<int, Task<IMd>> _creator;

        public static void SetLocator(Func<MdLocator, Task<Result<IMd>>> locator)
        {
            _locator = locator;
        }

        public static void SetCreator(Func<int, Task<IMd>> creator)
        {
            _creator = creator;
        }

        public static Task<Result<IMd>> LocateAsync(MdLocator location)
        {
            return _locator(location);
        }

        public static Task<IMd> CreateAsync(int level)
        {
            return _creator(level);
        }

        public static void UseInMemoryDb()
        {
            SetCreator(level => Task.FromResult(Md.Create(level)));
            SetLocator(location => Task.FromResult(Result.OK(Md.Locate(location))));
        }
    }
}
