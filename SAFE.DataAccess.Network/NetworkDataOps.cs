using SafeApp;
using SafeApp.Misc;
using SafeApp.Utilities;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess.Network
{
    public class NetworkDataOps
    {
        protected Session _session;

        public NetworkDataOps(Session session)
        {
            _session = session;
        }

        protected async Task<MDataInfo> CreateEmptyMd(ulong typeTag)
        {
            using (var permissionH = await _session.MDataPermissions.NewAsync())
            {
                using (var appSignPkH = await _session.Crypto.AppPubSignKeyAsync())
                    await _session.MDataPermissions.InsertAsync(permissionH, appSignPkH, GetFullPermissions());

                var info = await _session.MDataInfoActions.RandomPrivateAsync(typeTag);
                await _session.MData.PutAsync(info, permissionH, NativeHandle.Zero); // <----------------------------------------------    Commit ------------------------
                return info;
            }
        }

        protected async Task<List<byte>> CreateEmptyMdSerialized(ulong typeTag)
        {
            using (var permissionH = await _session.MDataPermissions.NewAsync())
            {
                using (var appSignPkH = await _session.Crypto.AppPubSignKeyAsync())
                    await _session.MDataPermissions.InsertAsync(permissionH, appSignPkH, GetFullPermissions());

                var info = await _session.MDataInfoActions.RandomPrivateAsync(typeTag);
                await _session.MData.PutAsync(info, permissionH, NativeHandle.Zero); // <----------------------------------------------    Commit ------------------------
                return await _session.MDataInfoActions.SerialiseAsync(info);
            }
        }

        // Populates the entries.
        protected async Task InsertDataEntries(NativeHandle mdEntriesH, Dictionary<string, List<byte>> data)
        {
            foreach (var pair in data)
                await _session.MDataEntries.InsertAsync(mdEntriesH, pair.Key.ToUtfBytes(), pair.Value);
        }

        #region MDEntries tx

        // Populate the md entry actions handle.
        protected async Task InsertEntriesAsync(NativeHandle entryActionsH, Dictionary<string, object> data)
        {
            foreach (var pair in data)
                await _session.MDataEntryActions.InsertAsync(entryActionsH, pair.Key.ToUtfBytes(), pair.Value.Json().ToUtfBytes());
        }

        // Populate the md entry actions handle.
        protected async Task UpdateEntriesAsync(NativeHandle entryActionsH, Dictionary<string, (object, ulong)> data)
        {
            foreach (var pair in data)
            {
                var val = pair.Value.Item1;
                var version = pair.Value.Item2;
                await _session.MDataEntryActions.UpdateAsync(entryActionsH, pair.Key.ToUtfBytes(), val.Json().ToUtfBytes(), version);
            }
        }

        // Populate the md entry actions handle.
        protected async Task DeleteEntriesAsync(NativeHandle entryActionsH, Dictionary<string, ulong> data)
        {
            foreach (var pair in data)
                await _session.MDataEntryActions.DeleteAsync(entryActionsH, pair.Key.ToUtfBytes(), pair.Value);
        }

        // Commit the operations in the md entry actions handle.
        protected async Task CommitEntryMutationAsync(MDataInfo mDataInfo, NativeHandle entryActionsH)
        {
            await _session.MData.MutateEntriesAsync(mDataInfo, entryActionsH); // <----------------------------------------------    Commit ------------------------
        }

        #endregion MDEntries tx

        // Creates with data.
        /// <summary>
        /// 
        /// </summary>
        /// <param name="permissionsHandle"></param>
        /// <param name="dataEntries"></param>
        /// <returns>A serialised MdInfo</returns>
        protected async Task<List<byte>> CreateRandomPrivateMd(NativeHandle permissionsHandle, NativeHandle dataEntries)
        {
            var info = await _session.MDataInfoActions.RandomPrivateAsync(15001); // todo: fix typetag
            await _session.MData.PutAsync(info, permissionsHandle, dataEntries); // <----------------------------------------------    Commit ------------------------
            return await _session.MDataInfoActions.SerialiseAsync(info);
        }

        /// <summary>
        /// Empty, without data.
        /// </summary>
        /// <param name="permissionsHandle"></param>
        /// <returns>SerialisedMdInfo</returns>
        protected async Task<List<byte>> CreateEmptyRandomPrivateMd(NativeHandle permissionsHandle)
        {
            return await CreateRandomPrivateMd(permissionsHandle, NativeHandle.Zero);
        }

        protected PermissionSet GetFullPermissions()
        {
            return new PermissionSet
            {
                Delete = true,
                Insert = true,
                ManagePermissions = true,
                Read = true,
                Update = true
            };
        }

        // Returns data map address.
        protected async Task<byte[]> StoreImmutableData(byte[] payload)
        {
            using (var cipherOptHandle = await _session.CipherOpt.NewPlaintextAsync())
            {
                using (var seWriterHandle = await _session.IData.NewSelfEncryptorAsync())
                {
                    await _session.IData.WriteToSelfEncryptorAsync(seWriterHandle, payload.ToList());
                    var dataMapAddress = await _session.IData.CloseSelfEncryptorAsync(seWriterHandle, cipherOptHandle);
                    return dataMapAddress;
                }
            }
        }

        protected static async Task<byte[]> GetMdXorName(string plainTextId)
        {
            return (await Crypto.Sha3HashAsync(plainTextId.ToUtfBytes())).ToArray();
        }


        protected async Task<(byte[], byte[])> GenerateRandomKeyPair()
        {
            var randomKeyPairTuple = await _session.Crypto.EncGenerateKeyPairAsync();
            byte[] encPublicKey, encSecretKey;
            using (var inboxEncPkH = randomKeyPairTuple.Item1)
            {
                using (var inboxEncSkH = randomKeyPairTuple.Item2)
                {
                    encPublicKey = await _session.Crypto.EncPubKeyGetAsync(inboxEncPkH);
                    encSecretKey = await _session.Crypto.EncSecretKeyGetAsync(inboxEncSkH);
                }
            }
            return (encPublicKey, encSecretKey);
        }
    }
}
