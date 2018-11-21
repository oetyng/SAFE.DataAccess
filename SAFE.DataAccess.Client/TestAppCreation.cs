using SafeApp;
using SafeApp.MockAuthBindings;
using SafeApp.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.DataAccess.Client
{
    public class TestAppCreation
    {
        static Random _random = new Random(new Random().Next());

        public static Task<Result<Session>> CreateTestApp(string appId)
        {
            var locator = GetRandomString(10);
            var secret = GetRandomString(10);
            var authReq = new AuthReq
            {
                App = new AppExchangeInfo { Id = appId, Name = GetRandomString(5), Scope = null, Vendor = GetRandomString(5) },
                AppContainer = true,
                Containers = new List<ContainerPermissions>()
            };
            return CreateTestApp(locator, secret, authReq);
        }

        public static Task<Result<Session>> CreateTestApp()
        {
            var locator = GetRandomString(10);
            var secret = GetRandomString(10);
            var authReq = new AuthReq
            {
                App = new AppExchangeInfo { Id = GetRandomString(10), Name = GetRandomString(5), Scope = null, Vendor = GetRandomString(5) },
                AppContainer = true,
                Containers = new List<ContainerPermissions>()
            };
            return CreateTestApp(locator, secret, authReq);
        }

        public static Task<Result<Session>> CreateTestApp(AuthReq authReq)
        {
            var locator = GetRandomString(10);
            var secret = GetRandomString(10);
            return CreateTestApp(locator, secret, authReq);
        }

        public static async Task<Result<Session>> CreateTestApp(string locator, string secret, AuthReq authReq)
        {
            var authenticator = await Authenticator.CreateAccountAsync(locator, secret, GetRandomString(5));
            var (_, reqMsg) = await Session.EncodeAuthReqAsync(authReq);
            var ipcReq = await authenticator.DecodeIpcMessageAsync(reqMsg);
            if (ipcReq.GetType() != typeof(AuthIpcReq))
                return new InvalidOperation<Session>($"Could not get {nameof(AuthIpcReq)}");

            var authIpcReq = ipcReq as AuthIpcReq;
            var resMsg = await authenticator.EncodeAuthRespAsync(authIpcReq, true);
            var ipcResponse = await Session.DecodeIpcMessageAsync(resMsg);
            var authResponse = ipcResponse as AuthIpcMsg;
            if (authResponse == null)
                return new InvalidOperation<Session>($"Could not get {nameof(AuthIpcMsg)}");

            authenticator.Dispose();
            var session = await Session.AppRegisteredAsync(authReq.App.Id, authResponse.AuthGranted);
            return Result.OK(session);
        }

        public static string GetRandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length).Select(s => s[_random.Next(s.Length)]).ToArray());
        }
    }
}
