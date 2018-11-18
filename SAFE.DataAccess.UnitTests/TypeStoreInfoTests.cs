using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace SAFE.DataAccess.UnitTests
{
    [TestClass]
    public class TypeStoreInfoTests
    {
        [TestInitialize]
        public void TestInitialize()
        {
            MdAccess.SetCreator(level => Task.FromResult(Md.Create(level)));
            MdAccess.SetLocator(xor => Task.FromResult(Md.Locate(xor)));
        }
    }
}
