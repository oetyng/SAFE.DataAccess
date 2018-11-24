using Microsoft.VisualStudio.TestTools.UnitTesting;
using SAFE.DataAccess.Client;
using SafeApp;
using SafeApp.Utilities;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SAFE.DataAccess.UnitTests
{
    [TestClass]
    public class MockVaultThrougPutTests
    {
        const string METADATA_KEY = "metadata";
        List<byte> METADATA_KEY_BYTES;
        
        Session _session;
        Network.NetworkDataOps _networkOps;

        MdMetadata _metadata;
        [TestInitialize]
        public async Task TestInitialize()
        {
            _session = (await TestAppCreation.CreateTestApp()).Value;
            _networkOps = new Network.NetworkDataOps(_session);
            METADATA_KEY_BYTES = METADATA_KEY.ToUtfBytes();
        }

        [TestMethod]
        public async Task Insert_performance()
        {
            try
            {
                ////for (int i = 0; i < 10000; i++)
                ////    await FillMd(i);
            }
            catch (Exception ex)
            {
                Assert.Fail();
            }
        }

        async Task FillMd(int iteration)
        {
            iteration *= 1000;
            var sw = new Stopwatch();
            var md = await GetNewMd().ConfigureAwait(false);
            await NewMetadata(md);
            for (int i = 0; i < MdMetadata.Capacity; i++)
            {
                var key = $"theKey_{i}";
                var obj = new StoredValue(i);
                sw.Restart();
                await AddObjectAsync(md, key, obj).ConfigureAwait(false);
                sw.Stop();
                Debug.WriteLine($"{i + iteration + 1}: {sw.ElapsedMilliseconds}");
            }
            _metadata = null;
        }

        [TestMethod]
        public void Parallel_Insert_performance()
        {
            try
            {
                int count = 0;
                int increment() => Interlocked.Increment(ref count);
                Parallel.ForEach(Enumerable.Range(0, 4), i =>
                {
                    ThreadedFillMd(increment).GetAwaiter().GetResult();
                });
            }
            catch (Exception ex)
            {
                Assert.Fail();
            }
        }

        async Task ThreadedFillMd(Func<int> incrCount)
        {
            var sw = new Stopwatch();
            var md = await GetNewMd().ConfigureAwait(false);
            await NewMetadata(md);
            for (int i = 0; i < MdMetadata.Capacity; i++)
            {
                var key = $"theKey_{i}";
                var obj = new StoredValue(i);
                sw.Restart();
                await AddObjectAsync(md, key, obj).ConfigureAwait(false);
                sw.Stop();
                Debug.WriteLine($"{incrCount()}: {sw.ElapsedMilliseconds}");
            }
            _metadata = null;
        }

        async Task<MDataInfo> GetNewMd()
        {
            using (var permissionsHandle = await _session.MDataPermissions.NewAsync().ConfigureAwait(false))
            {
                using (var appSignPkH = await _session.Crypto.AppPubSignKeyAsync().ConfigureAwait(false))
                {
                    await _session.MDataPermissions.InsertAsync(permissionsHandle, appSignPkH, _networkOps.GetFullPermissions()).ConfigureAwait(false);
                }

                var md = await _session.MDataInfoActions.RandomPublicAsync(16001).ConfigureAwait(false);

                await _session.MData.PutAsync(md, permissionsHandle, NativeHandle.Zero).ConfigureAwait(false); // <----------------------------------------------    Commit ------------------------
                return md;
            }
        }

        async Task AddObjectAsync(MDataInfo md, string key, object value)
        {
            using (var entryActionsH = await _session.MDataEntryActions.NewAsync().ConfigureAwait(false))
            {
                // insert value
                var insertObj = new Dictionary<string, object>
                {
                    { key, value }
                };
                await _networkOps.InsertEntriesAsync(entryActionsH, insertObj).ConfigureAwait(false);

                // commit
                await _networkOps.CommitEntryMutationAsync(md, entryActionsH).ConfigureAwait(false);
            }
        }

        async Task NewMetadata(MDataInfo md)
        {
            var keys = await _session.MData.ListKeysAsync(md).ConfigureAwait(false);
            if (keys.Count > 0)
            {
                var metaMD = await _session.MData.GetValueAsync(md, METADATA_KEY_BYTES).ConfigureAwait(false);
                var level = metaMD.Item1.ToUtfString().Parse<int>();
                return;
            }

            var insertObj = new Dictionary<string, object>
            {
                { METADATA_KEY, 0 }
            };
            using (var entryActionsH = await _session.MDataEntryActions.NewAsync().ConfigureAwait(false))
            {
                await _networkOps.InsertEntriesAsync(entryActionsH, insertObj).ConfigureAwait(false);
                await _networkOps.CommitEntryMutationAsync(md, entryActionsH).ConfigureAwait(false);
            }
        }
    }
}
