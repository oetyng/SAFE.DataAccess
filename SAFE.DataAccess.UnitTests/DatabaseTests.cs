using Microsoft.VisualStudio.TestTools.UnitTesting;
using SAFE.DataAccess.Client;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess.UnitTests
{
    [TestClass]
    public class DatabaseTests
    {
        IClient _client;

        //[TestInitialize]
        //public void TestInitialize()
        //{
        //    _client = ClientFactory.GetInMemoryClient();
        //}

        [TestInitialize]
        public async Task TestInitialize()
        {
            _client = await ClientFactory.GetMockNetworkClient();
        }

        [TestMethod]
        public async Task DatabaseTests_getoradd_returns_database()
        {
            // Act
            var dbResult = await GetDatabase("theDb");

            // Assert
            Assert.IsNotNull(dbResult);
            Assert.IsInstanceOfType(dbResult, typeof(Result<Database>));
            Assert.IsTrue(dbResult.HasValue);
        }

        [TestMethod]
        public async Task DatabaseTests_add_returns_pointer()
        {
            // Arrange
            var dbResult = await GetDatabase("theDb");
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
            var dbResult = await GetDatabase("theDb");
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
            var dbResult = await GetDatabase("theDb");
            var addCount = Math.Round(1.3 * MdMetadata.Capacity);
            var sw = new Stopwatch();

            for (int i = 0; i < addCount; i++)
            {
                
                var theKey = $"theKey_{i}";
                var theData = i;

                // Act
                sw.Restart();
                var addResult = await dbResult.Value.AddAsync(theKey, theData).ConfigureAwait(false);
                sw.Stop();

                if (!addResult.HasValue)
                { }

                // Assert 1
                Assert.IsNotNull(addResult);
                Assert.IsInstanceOfType(addResult, typeof(Result<Pointer>));
                Assert.IsTrue(addResult.HasValue);
                Debug.WriteLine($"{i}: {sw.ElapsedMilliseconds}");
            }

            // Assert 2
            var data = (await dbResult.Value.GetAllAsync<int>().ConfigureAwait(false)).ToList();
            Assert.IsNotNull(data);
            Assert.AreEqual(addCount, data.Count);
        }

        Task<Result<Database>> GetDatabase(string dbName)
        {
            return _client.GetOrAddDataBaseAsync(dbName);
        }
    }
}
