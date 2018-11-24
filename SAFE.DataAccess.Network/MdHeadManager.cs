using SafeApp;
using SafeApp.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess.Network
{
    public class MdHeadManager : NetworkDataOps
    {
        readonly string AppContainerPath;
        readonly ulong _protocol;
        MdContainer _mdContainer;
        ulong _mdContainerVersion;

        public MdHeadManager(Session session, string appId, ulong protocol)
            : base(session)
        {
            AppContainerPath = $"apps/{appId}";
            _protocol = protocol;
        }

        public async Task InitializeManager()
        {
            if (!await ExistsManagerAsync())
            {
                // Create new md head container
                _mdContainer = new MdContainer();
                var serializedDbContainer = _mdContainer.Json();

                // Update App Container (store md head info to it)
                var appContainer = await Session.AccessContainer.GetMDataInfoAsync(AppContainerPath);
                var dbIdCipherBytes = await Session.MDataInfoActions.EncryptEntryKeyAsync(appContainer, nameof(MdContainer).ToUtfBytes());
                var dbCipherBytes = await Session.MDataInfoActions.EncryptEntryValueAsync(appContainer, serializedDbContainer.ToUtfBytes());
                using (var appContEntryActionsH = await Session.MDataEntryActions.NewAsync())
                {
                    await Session.MDataEntryActions.InsertAsync(appContEntryActionsH, dbIdCipherBytes, dbCipherBytes);
                    await Session.MData.MutateEntriesAsync(appContainer, appContEntryActionsH); // <----------------------------------------------    Commit ------------------------
                }
            }
            else
                await LoadDbContainer();
        }

        public async Task<MdHead> GetOrAddHeadAsync(string mdName)
        {
            if (mdName.Contains(".") || mdName.Contains("@"))
                throw new NotSupportedException("Unsupported characters '.' and '@'.");

            var mdId = $"{_protocol}/{mdName}";

            if (_mdContainer.MdLocators.ContainsKey(mdId))
            {
                var location = _mdContainer.MdLocators[mdId];
                var mdResult = await LocateMdOps(location);
                return new MdHead(mdResult.Value, mdId);
            }

            // Create Permissions
            using (var permissionsHandle = await Session.MDataPermissions.NewAsync())
            {
                using (var appSignPkH = await Session.Crypto.AppPubSignKeyAsync())
                {
                    await Session.MDataPermissions.InsertAsync(permissionsHandle, appSignPkH, GetFullPermissions());
                }

                var mDataInfo = await CreateEmptyRandomPrivateMd(permissionsHandle, DataProtocol.DEFAULT_PROTOCOL);// TODO: DataProtocol.MD_HEAD);
                var location = new MdLocator(mDataInfo.Name, mDataInfo.TypeTag);
                _mdContainer.MdLocators[mdId] = location;

                // Finally update App Container (store md head info to it)
                var serializedMdContainer = _mdContainer.Json();
                var appContainer = await Session.AccessContainer.GetMDataInfoAsync(AppContainerPath);
                var mdKeyCipherBytes = await Session.MDataInfoActions.EncryptEntryKeyAsync(appContainer, nameof(MdContainer).ToUtfBytes());
                var mdCipherBytes = await Session.MDataInfoActions.EncryptEntryValueAsync(appContainer, serializedMdContainer.ToUtfBytes());
                using (var appContEntryActionsH = await Session.MDataEntryActions.NewAsync())
                {
                    await Session.MDataEntryActions.UpdateAsync(appContEntryActionsH, mdKeyCipherBytes, mdCipherBytes, _mdContainerVersion + 1);
                    await Session.MData.MutateEntriesAsync(appContainer, appContEntryActionsH); // <----------------------------------------------    Commit ------------------------
                }

                ++_mdContainerVersion;

                var mdResult = await LocateMdOps(location);
                return new MdHead(mdResult.Value, mdId);
            }
        }

        public Task<IMd> CreateNewMdOps(int level, ulong protocol)
        {
            return MdOps.CreateNewMdOpsAsync(level, new NetworkDataOps(Session), protocol);
        }

        public Task<Result<IMd>> LocateMdOps(MdLocator location)
        {
            return MdOps.LocateAsync(location, new NetworkDataOps(Session));
        }

        async Task<bool> ExistsManagerAsync()
        {
            var appCont = await Session.AccessContainer.GetMDataInfoAsync(AppContainerPath);
            var mdKeyCipherBytes = await Session.MDataInfoActions.EncryptEntryKeyAsync(appCont, nameof(MdContainer).ToUtfBytes());
            var keys = await Session.MData.ListKeysAsync(appCont);
            return keys.Any(c => c.Val.SequenceEqual(mdKeyCipherBytes));
        }

        async Task<MdContainer> LoadDbContainer()
        {
            var appContainerInfo = await Session.AccessContainer.GetMDataInfoAsync(AppContainerPath);
            var mdKeyCipherBytes = await Session.MDataInfoActions.EncryptEntryKeyAsync(appContainerInfo, nameof(MdContainer).ToUtfBytes());
            var cipherTxtEntryVal = await Session.MData.GetValueAsync(appContainerInfo, mdKeyCipherBytes);

            var plainTxtEntryVal = await Session.MDataInfoActions.DecryptAsync(appContainerInfo, cipherTxtEntryVal.Item1);
            var mdContainerJson = plainTxtEntryVal.ToUtfString();
            var mdContainer = mdContainerJson.Parse<MdContainer>();
            _mdContainer = mdContainer;
            return mdContainer;
        }

        class MdContainer
        {
            public Dictionary<string, MdLocator> MdLocators { get; set; } = new Dictionary<string, MdLocator>();
        }
    }
}
