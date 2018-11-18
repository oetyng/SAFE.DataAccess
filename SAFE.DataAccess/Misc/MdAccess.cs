using System;

namespace SAFE.DataAccess
{
    public class MdAccess
    {
        static Func<byte[], IMd> _locator;
        static Func<int, IMd> _creator;

        public static void SetLocator(Func<byte[], IMd> locator)
        {
            _locator = locator;
        }

        public static void SetCreator(Func<int, IMd> creator)
        {
            _creator = creator;
        }

        public static IMd Locate(byte[] xorAddress)
        {
            return _locator(xorAddress);
        }

        public static IMd Create(int level)
        {
            return _creator(level);
        }
    }
}
