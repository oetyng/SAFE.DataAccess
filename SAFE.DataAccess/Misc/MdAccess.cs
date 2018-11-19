using System;
using System.Threading.Tasks;

namespace SAFE.DataAccess
{
    public class MdAccess
    {
        static Func<byte[], Task<IMd>> _locator;
        static Func<int, Task<IMd>> _creator;

        public static void SetLocator(Func<byte[], Task<IMd>> locator)
        {
            _locator = locator;
        }

        public static void SetCreator(Func<int, Task<IMd>> creator)
        {
            _creator = creator;
        }

        public static Task<IMd> LocateAsync(byte[] xorAddress)
        {
            return _locator(xorAddress);
        }

        public static Task<IMd> CreateAsync(int level)
        {
            return _creator(level);
        }

        public static void UseInMemoryDb()
        {
            SetCreator(level => Task.FromResult(Md.Create(level)));
            SetLocator(xor => Task.FromResult(Md.Locate(xor)));
        }
    }
}
