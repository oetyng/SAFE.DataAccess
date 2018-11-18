using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace SAFE.DataAccess.UnitTests
{
    [TestClass]
    public class DatabaseTests
    {
        [TestInitialize]
        public void TestInitialize()
        {
            MdAccess.SetCreator(Md.Create);
            MdAccess.SetLocator(Md.Locate);
        }

        [TestMethod]
        public void DatabaseTests_getoradd_returns_database()
        {
            // Arrange
            var dbId = "theDb";
            var indexer = Indexer.Create(dbId);

            // Act
            var dbResult = Database.GetOrAdd(dbId, indexer);

            // Assert
            Assert.IsNotNull(dbResult);
            Assert.IsInstanceOfType(dbResult, typeof(Result<Database>));
            Assert.IsTrue(dbResult.HasValue);
        }

        [TestMethod]
        public void DatabaseTests_add_returns_pointer()
        {
            // Arrange
            var dbId = "theDb";
            var indexer = Indexer.Create(dbId);
            var dbResult = Database.GetOrAdd(dbId, indexer);
            var theKey = "theKey";
            var theData = 42;

            // Act
            var addResult = dbResult.Value.Add(theKey, theData);

            // Assert
            Assert.IsNotNull(addResult);
            Assert.IsInstanceOfType(addResult, typeof(Result<Pointer>));
            Assert.IsTrue(addResult.HasValue);
        }

        [TestMethod]
        public void DatabaseTests_returns_stored_value()
        {
            // Arrange
            var dbId = "theDb";
            var indexer = Indexer.Create(dbId);
            var dbResult = Database.GetOrAdd(dbId, indexer);
            var theKey = "theKey";
            var theData = 42;
            var addResult = dbResult.Value.Add(theKey, theData);

            // Act
            var findResult = dbResult.Value.FindByKey<int>(theKey);

            // Assert
            Assert.IsNotNull(findResult);
            Assert.IsInstanceOfType(findResult, typeof(Result<int>));
            Assert.IsTrue(findResult.HasValue);
            Assert.AreEqual(theData, findResult.Value);
        }

        [TestMethod]
        public void DatabaseTests_adds_more_than_md_capacity()
        {
            // Arrange
            var dbId = "theDb";
            var indexer = Indexer.Create(dbId);
            var dbResult = Database.GetOrAdd(dbId, indexer);
            var addCount = 5.3 * Metadata.Capacity;

            for (int i = 0; i < addCount; i++)
            {
                var theKey = $"theKey_{i}";
                var theData = i;

                // Act
                var addResult = dbResult.Value.Add(theKey, theData);

                // Assert 1
                Assert.IsNotNull(addResult);
                Assert.IsInstanceOfType(addResult, typeof(Result<Pointer>));
                Assert.IsTrue(addResult.HasValue);
            }

            // Assert 2
            var data = dbResult.Value.GetAll<int>().ToList();
            Assert.IsNotNull(data);
            Assert.AreEqual(addCount, data.Count);
        }
    }
}
