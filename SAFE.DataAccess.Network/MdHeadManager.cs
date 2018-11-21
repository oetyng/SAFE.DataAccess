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
                // Create new db container
                _mdContainer = new MdContainer();
                var serializedDbContainer = _mdContainer.Json();

                // Update App Container (store db info to it)
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

            if (_mdContainer.MdLocations.ContainsKey(mdId))
            {
                var location = _mdContainer.MdLocations[mdId];
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

                // Create one Md for holding category partition info
                var mDataInfo = await CreateEmptyRandomPrivateMd(permissionsHandle, DataProtocol.DEFAULT_PROTOCOL);// TODO: DataProtocol.MD_HEAD);
                var location = new MdLocation(mDataInfo.Name, mDataInfo.TypeTag);
                _mdContainer.MdLocations[mdId] = location;

                // Finally update App Container (store db info to it)

                var serializedDbContainer = _mdContainer.Json();
                var appContainer = await Session.AccessContainer.GetMDataInfoAsync(AppContainerPath);
                var dbIdCipherBytes = await Session.MDataInfoActions.EncryptEntryKeyAsync(appContainer, nameof(MdContainer).ToUtfBytes());
                var dbCipherBytes = await Session.MDataInfoActions.EncryptEntryValueAsync(appContainer, serializedDbContainer.ToUtfBytes());
                using (var appContEntryActionsH = await Session.MDataEntryActions.NewAsync())
                {
                    await Session.MDataEntryActions.UpdateAsync(appContEntryActionsH, dbIdCipherBytes, dbCipherBytes, _mdContainerVersion + 1);
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

        public Task<Result<IMd>> LocateMdOps(MdLocation location)
        {
            return MdOps.LocateAsync(location, new NetworkDataOps(Session));
        }

        //async Task<IMd> GetMdAsync(List<byte> addressBytes)
        //{
        //    var mdInfo = await _session.MDataInfoActions.DeserialiseAsync(addressBytes);
        //    var md = new MdOps(mdInfo, _session);
        //    await md.Initialize(level: 0);
        //    return md;
        //}

        async Task<bool> ExistsManagerAsync()
        {
            var appCont = await Session.AccessContainer.GetMDataInfoAsync(AppContainerPath);
            var dbIdCipherBytes = await Session.MDataInfoActions.EncryptEntryKeyAsync(appCont, nameof(MdContainer).ToUtfBytes());
            var keys = await Session.MData.ListKeysAsync(appCont);
            return keys.Any(c => c.Val.SequenceEqual(dbIdCipherBytes));
        }

        async Task<MdContainer> LoadDbContainer()
        {
            var appContainerInfo = await Session.AccessContainer.GetMDataInfoAsync(AppContainerPath);
            var dbIdCipherBytes = await Session.MDataInfoActions.EncryptEntryKeyAsync(appContainerInfo, nameof(MdContainer).ToUtfBytes());
            var cipherTxtEntryVal = await Session.MData.GetValueAsync(appContainerInfo, dbIdCipherBytes);

            var plainTxtEntryVal = await Session.MDataInfoActions.DecryptAsync(appContainerInfo, cipherTxtEntryVal.Item1);
            var dbContainerJson = plainTxtEntryVal.ToUtfString();
            var dbContainer = dbContainerJson.Parse<MdContainer>();
            _mdContainer = dbContainer;
            return dbContainer;
        }

        class MdContainer
        {
            public Dictionary<string, MdLocation> MdLocations { get; set; } = new Dictionary<string, MdLocation>();
        }
    }
}
