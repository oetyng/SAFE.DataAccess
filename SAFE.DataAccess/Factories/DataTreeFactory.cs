using System;
using System.Threading.Tasks;

namespace SAFE.DataAccess.Factories
{
    public class DataTreeFactory
    {
        public static async Task<DataTree> CreateAsync(Func<MdLocation, Task> onHeadAddressChange)
        {
            var head = await MdAccess.CreateAsync(level: 0);
            var dataTree = new DataTree(head, onHeadAddressChange);
            return dataTree;
        }
    }
}
