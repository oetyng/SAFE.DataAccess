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
        public readonly Session Session;

        public NetworkDataOps(Session session)
        {
            Session = session;
        }

        public async Task<MDataInfo> CreateEmptyMd(ulong typeTag)
        {
            using (var permissionH = await Session.MDataPermissions.NewAsync())
            {
                using (var appSignPkH = await Session.Crypto.AppPubSignKeyAsync())
                    await Session.MDataPermissions.InsertAsync(permissionH, appSignPkH, GetFullPermissions());

                var info = await Session.MDataInfoActions.RandomPrivateAsync(typeTag);
                await Session.MData.PutAsync(info, permissionH, NativeHandle.Zero); // <----------------------------------------------    Commit ------------------------
                return info;
            }
        }

        public async Task<List<byte>> CreateEmptyMdSerialized(ulong typeTag)
        {
            using (var permissionH = await Session.MDataPermissions.NewAsync())
            {
                using (var appSignPkH = await Session.Crypto.AppPubSignKeyAsync())
                    await Session.MDataPermissions.InsertAsync(permissionH, appSignPkH, GetFullPermissions());

                var info = await Session.MDataInfoActions.RandomPrivateAsync(typeTag);
                await Session.MData.PutAsync(info, permissionH, NativeHandle.Zero); // <----------------------------------------------    Commit ------------------------
                return await Session.MDataInfoActions.SerialiseAsync(info);
            }
        }

        // Populates the entries.
        public async Task InsertDataEntries(NativeHandle mdEntriesH, Dictionary<string, List<byte>> data)
        {
            foreach (var pair in data)
                await Session.MDataEntries.InsertAsync(mdEntriesH, pair.Key.ToUtfBytes(), pair.Value);
        }

        #region MDEntries tx

        // Populate the md entry actions handle.
        public async Task InsertEntriesAsync(NativeHandle entryActionsH, Dictionary<string, object> data)
        {
            foreach (var pair in data)
                await Session.MDataEntryActions.InsertAsync(entryActionsH, pair.Key.ToUtfBytes(), pair.Value.Json().ToUtfBytes());
        }

        // Populate the md entry actions handle.
        public async Task UpdateEntriesAsync(NativeHandle entryActionsH, Dictionary<string, (object, ulong)> data)
        {
            foreach (var pair in data)
            {
                var val = pair.Value.Item1;
                var version = pair.Value.Item2;
                await Session.MDataEntryActions.UpdateAsync(entryActionsH, pair.Key.ToUtfBytes(), val.Json().ToUtfBytes(), version);
            }
        }

        // Populate the md entry actions handle.
        public async Task DeleteEntriesAsync(NativeHandle entryActionsH, Dictionary<string, ulong> data)
        {
            foreach (var pair in data)
                await Session.MDataEntryActions.DeleteAsync(entryActionsH, pair.Key.ToUtfBytes(), pair.Value);
        }

        // Commit the operations in the md entry actions handle.
        public async Task CommitEntryMutationAsync(MDataInfo mDataInfo, NativeHandle entryActionsH)
        {
            await Session.MData.MutateEntriesAsync(mDataInfo, entryActionsH); // <----------------------------------------------    Commit ------------------------
        }

        #endregion MDEntries tx

        // Creates with data.
        /// <summary>
        /// 
        /// </summary>
        /// <param name="permissionsHandle"></param>
        /// <param name="dataEntries"></param>
        /// <returns>A serialised MdInfo</returns>
        public async Task<MDataInfo> CreateRandomPrivateMd(NativeHandle permissionsHandle, NativeHandle dataEntries, ulong protocol)
        {
            var mdInfo = await Session.MDataInfoActions.RandomPrivateAsync(protocol);
            await Session.MData.PutAsync(mdInfo, permissionsHandle, dataEntries); // <----------------------------------------------    Commit ------------------------
            return mdInfo;
        }

        /// <summary>
        /// Empty, without data.
        /// </summary>
        /// <param name="permissionsHandle"></param>
        /// <returns>SerialisedMdInfo</returns>
        public async Task<MDataInfo> CreateEmptyRandomPrivateMd(NativeHandle permissionsHandle, ulong protocol)
        {
            return await CreateRandomPrivateMd(permissionsHandle, NativeHandle.Zero, protocol);
        }

        public async Task<Result<MDataInfo>> LocatePublicMd(byte[] xor, ulong protocol)
        {
            var md = new MDataInfo { Name = xor, TypeTag = protocol };

            try
            {
                await Session.MData.ListKeysAsync(md);
            }
            catch(System.Exception ex)
            {
                return new KeyNotFound<MDataInfo>($"Could not find Md with tag type {protocol} and address {xor}");
            }

            return Result.OK(md);
        }

        public PermissionSet GetFullPermissions()
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
        public async Task<byte[]> StoreImmutableData(byte[] payload)
        {
            using (var cipherOptHandle = await Session.CipherOpt.NewPlaintextAsync())
            {
                using (var seWriterHandle = await Session.IData.NewSelfEncryptorAsync())
                {
                    await Session.IData.WriteToSelfEncryptorAsync(seWriterHandle, payload.ToList());
                    var dataMapAddress = await Session.IData.CloseSelfEncryptorAsync(seWriterHandle, cipherOptHandle);
                    return dataMapAddress;
                }
            }
        }

        public static async Task<byte[]> GetMdXorName(string plainTextId)
        {
            return (await Crypto.Sha3HashAsync(plainTextId.ToUtfBytes())).ToArray();
        }


        public async Task<(byte[], byte[])> GenerateRandomKeyPair()
        {
            var randomKeyPairTuple = await Session.Crypto.EncGenerateKeyPairAsync();
            byte[] encPublicKey, encSecretKey;
            using (var inboxEncPkH = randomKeyPairTuple.Item1)
            {
                using (var inboxEncSkH = randomKeyPairTuple.Item2)
                {
                    encPublicKey = await Session.Crypto.EncPubKeyGetAsync(inboxEncPkH);
                    encSecretKey = await Session.Crypto.EncSecretKeyGetAsync(inboxEncSkH);
                }
            }
            return (encPublicKey, encSecretKey);
        }
    }
}
