using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace SAFE.DataAccess.FileSystems.UnitTests
{
    public static class EAssert
    {
        public static void Throws<T>(Action a)
            where T : Exception
        {
            try
            {
                a();
            }
            catch (T)
            {
                return;
            }
            Assert.Fail(string.Format("The exception '{0}' was not thrown.", typeof(T).FullName));
        }
    }
}
