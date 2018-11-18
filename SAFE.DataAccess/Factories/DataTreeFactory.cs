using System;

namespace SAFE.DataAccess.Factories
{
    public class DataTreeFactory
    {
        public static DataTree Create(Action<byte[]> onHeadAddressChange)
        {
            var head = Md.Create(level: 0);
            var dataTree = new DataTree(head, onHeadAddressChange);
            return dataTree;
        }
    }
}
