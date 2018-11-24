using Microsoft.VisualStudio.TestTools.UnitTesting;
using SAFE.DataAccess.Client;
using SafeApp;
using SafeApp.Utilities;
using System;
using System.Threading.Tasks;

namespace SAFE.DataAccess.UnitTests
{
    [TestClass]
    public class MDataInfoTests
    {
        Session _session;

        [TestInitialize]
        public async Task TestInitialize()
        {
            _session = (await TestAppCreation.CreateTestApp()).Value;
        }

        [TestMethod]
        public async Task PublicMd_IsFound_When_TagType_IsSame()
        {
            var (xor, tag) = await MdLocator(_session, pub: true);
            var dstPubIdMDataInfoH = new MDataInfo { Name = xor, TypeTag = tag };
            try
            {
                var keys = await _session.MData.ListKeysAsync(dstPubIdMDataInfoH);
                Assert.IsNotNull(keys);
            }
            catch (Exception ex)
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public async Task PublicMd_IsNOTFound_When_TagType_IsNotSame()
        {
            var (xor, tag) = await MdLocator(_session, pub: true);
            var dstPubIdMDataInfoH = new MDataInfo { Name = xor, TypeTag = 15001 };
            try
            {
                var keys = await _session.MData.ListKeysAsync(dstPubIdMDataInfoH);
                Assert.Fail();
            }
            catch (Exception ex)
            { }
        }

        [TestMethod]
        public async Task PrivateMd_IsFound_When_TagType_IsSame()
        {
            var (xor, tag) = await MdLocator(_session);
            var dstPubIdMDataInfoH = new MDataInfo { Name = xor, TypeTag = tag };
            try
            {
                var keys = await _session.MData.ListKeysAsync(dstPubIdMDataInfoH);
                Assert.IsNotNull(keys);
            }
            catch(Exception ex)
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public async Task PrivateMd_IsNOTFound_When_TagType_IsNotSame()
        {
            var (xor, tag) = await MdLocator(_session);
            var dstPubIdMDataInfoH = new MDataInfo { Name = xor, TypeTag = 15001 };
            try
            {
                var keys = await _session.MData.ListKeysAsync(dstPubIdMDataInfoH);
                Assert.Fail();
            }
            catch (Exception ex)
            { }
        }

        async Task<(byte[], ulong)> MdLocator(Session session, bool pub = false)
        {
            var networkOps = new Network.NetworkDataOps(session);

            using (var permissionsHandle = await session.MDataPermissions.NewAsync())
            {
                using (var appSignPkH = await session.Crypto.AppPubSignKeyAsync())
                {
                    await session.MDataPermissions.InsertAsync(permissionsHandle, appSignPkH, networkOps.GetFullPermissions());
                }

                var md = pub ? 
                    await session.MDataInfoActions.RandomPublicAsync(16001) :
                     await session.MDataInfoActions.RandomPrivateAsync(16001);

                await session.MData.PutAsync(md, permissionsHandle, NativeHandle.Zero); // <----------------------------------------------    Commit ------------------------
                return (md.Name, md.TypeTag);
            }
        }
    }
}
