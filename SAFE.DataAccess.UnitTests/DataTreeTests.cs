using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SAFE.DataAccess.UnitTests
{
    [TestClass]
    public class DataTreeTests
    {
        [TestInitialize]
        public void TestInitialize()
        {
            MdAccess.SetCreator(Md.Create);
            MdAccess.SetLocator(Md.Locate);
        }
    }
}
