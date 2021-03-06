﻿using System;
using System.IO;
using System.Linq;
using System.Text;
using SAFE.DataAccess.FileSystems.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SAFE.DataAccess.Client;

namespace SAFE.DataAccess.FileSystems.UnitTests
{
    [TestClass]
    public class MdFileSystemTest
    {
        FileSystem FileSystem { get; set; }
        FileSystemPath RootFilePath { get; } = FileSystemPath.Root.AppendFile("x");

        [TestInitialize]
        public void TestInitialize()
        {
            //FileSystem = FileSystemFactory.CreateInMemory().Value;
            FileSystem = FileSystemFactory.MockSAFENetwork()
                .GetAwaiter()
                .GetResult()
                .Value;
        }

        [TestMethod]
        public void CreateFile()
        {
            // File shouldn’t exist prior to creation.
            Assert.IsFalse(FileSystem.Exists(RootFilePath));

            var content = new byte[] { 0xde, 0xad, 0xbe, 0xef, };
            using (var xStream = FileSystem.CreateFile(RootFilePath))
            {
                // File now should exist.
                Assert.IsTrue(FileSystem.Exists(RootFilePath));

                xStream.Write(content, 0, content.Length);
                //xStream.FlushAsync();
                xStream.Flush();
            }

            // File should still exist and have content.
            Assert.IsTrue(FileSystem.Exists(RootFilePath));
            using (var xStream = FileSystem.OpenFile(RootFilePath, FileAccess.Read))
            {
                var readContent = new byte[2 * content.Length];
                Assert.AreEqual(content.Length, xStream.Read(readContent, 0, readContent.Length));
                CollectionAssert.AreEqual(
                    content,
                    // trim to the length that was read.
                    readContent.Take(content.Length).ToArray());

                // Trying to read beyond end of file should return 0.
                Assert.AreEqual(0, xStream.Read(readContent, 0, readContent.Length));
            }
        }

        [TestMethod]
        public void CreateFile_Exists()
        {
            Assert.IsFalse(FileSystem.Exists(RootFilePath));

            // Place initial content.
            using (var stream = FileSystem.CreateFile(RootFilePath))
            {
                stream.Write(Encoding.UTF8.GetBytes("asdf"));
                stream.Flush();
            }

            Assert.IsTrue(FileSystem.Exists(RootFilePath));

            // Replace—truncates.
            var content = Encoding.UTF8.GetBytes("b");
            using (var stream = FileSystem.CreateFile(RootFilePath))
            {
                stream.Write(content);
                stream.Flush();
            }

            Assert.IsTrue(FileSystem.Exists(RootFilePath));
            using (var stream = FileSystem.OpenFile(RootFilePath, FileAccess.Read))
                CollectionAssert.AreEqual(content, stream.ReadAllBytes());
        }

        [TestMethod]
        public void CreateFile_Empty()
        {
            using (var stream = FileSystem.CreateFile(RootFilePath))
            {
            }

            Assert.IsTrue(FileSystem.Exists(RootFilePath));

            using (var stream = FileSystem.OpenFile(RootFilePath, FileAccess.Read))
            {
                CollectionAssert.AreEqual(
                    new byte[] { },
                    stream.ReadAllBytes());
            }
        }

        [TestMethod]
        public void GetEntities()
        {
            for (var i = 0; i < 10; i++)
            {
                Console.WriteLine($"i={i}");
                using (var stream = FileSystem.CreateFile(RootFilePath))
                {
                }

                // Should exist once.
                CollectionAssert.AreEquivalent(
                    new[] { RootFilePath, },
                    FileSystem.GetEntities(FileSystemPath.Root).ToArray());
            }
        }
    }
}