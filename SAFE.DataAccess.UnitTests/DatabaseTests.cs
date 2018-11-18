using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess.UnitTests
{
    [TestClass]
    public class DatabaseTests
    {
        [TestInitialize]
        public void TestInitialize()
        {
            MdAccess.SetCreator(level => Task.FromResult(Md.Create(level)));
            MdAccess.SetLocator(xor => Task.FromResult(Md.Locate(xor)));
        }

        [TestMethod]
        public async Task DatabaseTests_getoradd_returns_database()
        {
            // Arrange
            var dbId = "theDb";
            var indexer = await Indexer.CreateAsync(dbId).ConfigureAwait(false);

            // Act
            var dbResult = await Database.GetOrAddAsync(dbId, indexer).ConfigureAwait(false);

            // Assert
            Assert.IsNotNull(dbResult);
            Assert.IsInstanceOfType(dbResult, typeof(Result<Database>));
            Assert.IsTrue(dbResult.HasValue);
        }

        [TestMethod]
        public async Task DatabaseTests_add_returns_pointer()
        {
            // Arrange
            var dbId = "theDb";
            var indexer = await Indexer.CreateAsync(dbId).ConfigureAwait(false);
            var dbResult = await Database.GetOrAddAsync(dbId, indexer).ConfigureAwait(false);
            var theKey = "theKey";
            var theData = 42;

            // Act
            var addResult = await dbResult.Value.AddAsync(theKey, theData).ConfigureAwait(false);

            // Assert
            Assert.IsNotNull(addResult);
            Assert.IsInstanceOfType(addResult, typeof(Result<Pointer>));
            Assert.IsTrue(addResult.HasValue);
        }

        [TestMethod]
        public async Task DatabaseTests_returns_stored_value()
        {
            // Arrange
            var dbId = "theDb";
            var indexer = await Indexer.CreateAsync(dbId).ConfigureAwait(false);
            var dbResult = await Database.GetOrAddAsync(dbId, indexer).ConfigureAwait(false);
            var theKey = "theKey";
            var theData = 42;
            var addResult = await dbResult.Value.AddAsync(theKey, theData).ConfigureAwait(false);

            // Act
            var findResult = await dbResult.Value.FindByKeyAsync<int>(theKey).ConfigureAwait(false);

            // Assert
            Assert.IsNotNull(findResult);
            Assert.IsInstanceOfType(findResult, typeof(Result<int>));
            Assert.IsTrue(findResult.HasValue);
            Assert.AreEqual(theData, findResult.Value);
        }

        [TestMethod]
        public async Task DatabaseTests_adds_more_than_md_capacity()
        {
            // Arrange
            var dbId = "theDb";
            var indexer = await Indexer.CreateAsync(dbId).ConfigureAwait(false);
            var dbResult = await Database.GetOrAddAsync(dbId, indexer).ConfigureAwait(false);
            var addCount = 5.3 * MdMetadata.Capacity;

            for (int i = 0; i < addCount; i++)
            {
                var theKey = $"theKey_{i}";
                var theData = i;

                // Act
                var addResult = await dbResult.Value.AddAsync(theKey, theData).ConfigureAwait(false);

                // Assert 1
                Assert.IsNotNull(addResult);
                Assert.IsInstanceOfType(addResult, typeof(Result<Pointer>));
                Assert.IsTrue(addResult.HasValue);
            }

            // Assert 2
            var data = (await dbResult.Value.GetAllAsync<int>().ConfigureAwait(false)).ToList();
            Assert.IsNotNull(data);
            Assert.AreEqual(addCount, data.Count);
        }
    }
}
