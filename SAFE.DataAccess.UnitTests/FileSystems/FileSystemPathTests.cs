﻿using System.Collections.Generic;
using System.Linq;
using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SAFE.DataAccess.FileSystems.UnitTests
{
    /// <summary>
    ///This is a test class for FileSystemPathTest and is intended
    ///to contain all FileSystemPathTest Unit Tests
    ///</summary>
    [TestClass]
    public class FileSystemPathTest
    {
        private FileSystemPath[] _paths = new[]
                                                    {
                                                        root,
                                                        directoryA,
                                                        fileA,
                                                        directoryB,
                                                        fileB
                                                    };
        private IEnumerable<FileSystemPath> Directories { get { return _paths.Where(p => p.IsDirectory); } }
        private IEnumerable<FileSystemPath> Files { get { return _paths.Where(p => p.IsFile); } }

        private static readonly FileSystemPath directoryA = FileSystemPath.Parse("/directorya/");
        private static FileSystemPath fileA = FileSystemPath.Parse("/filea");
        private static FileSystemPath directoryB = FileSystemPath.Parse("/directorya/directoryb/");
        private static FileSystemPath fileB = FileSystemPath.Parse("/directorya/fileb.txt");
        private static FileSystemPath root = FileSystemPath.Root;
        private FileSystemPath fileC;

        public FileSystemPathTest()
        {

        }

        /// <summary>
        ///A test for Root
        ///</summary>
        [TestMethod]
        public void RootTest()
        {
            Assert.AreEqual(FileSystemPath.Parse("/"), root);
        }

        /// <summary>
        ///A test for ParentPath
        ///</summary>
        [TestMethod]
        public void ParentPathTest()
        {
            Assert.IsTrue(
                Directories
                    .Where(d => d.GetDirectorySegments().Length == 1)
                    .All(d => d.ParentPath == root)
                    );

            Assert.IsFalse(!Files.All(f => f.RemoveChild(root.AppendFile(f.EntityName)) == f.ParentPath));
            EAssert.Throws<InvalidOperationException>(() => Assert.AreEqual(root.ParentPath, root.ParentPath));
        }

        /// <summary>
        ///A test for IsRoot
        ///</summary>
        [TestMethod]
        public void IsRootTest()
        {
            Assert.IsTrue(root.IsRoot);
            Assert.IsFalse(directoryA.IsRoot);
            Assert.IsFalse(fileA.IsRoot);
        }

        /// <summary>
        ///A test for IsFile
        ///</summary>
        [TestMethod]
        public void IsFileTest()
        {

            Assert.IsTrue(fileA.IsFile);
            Assert.IsFalse(directoryA.IsFile);
            Assert.IsFalse(root.IsFile);
        }

        /// <summary>
        ///A test for IsDirectory
        ///</summary>
        [TestMethod]
        public void IsDirectoryTest()
        {
            Assert.IsTrue(directoryA.IsDirectory);
            Assert.IsTrue(root.IsDirectory);
            Assert.IsFalse(fileA.IsDirectory);
        }

        /// <summary>
        ///A test for EntityName
        ///</summary>
        [TestMethod]
        public void EntityNameTest()
        {
            Assert.AreEqual(fileA.EntityName, "filea");
            Assert.AreEqual(fileB.EntityName, "fileb.txt");
            Assert.AreEqual(root.EntityName, null);
        }

        /// <summary>
        ///A test for ToString
        ///</summary>
        [TestMethod]
        public void ToStringTest()
        {
            string s = "/directorya/";
            Assert.AreEqual(s, FileSystemPath.Parse(s).ToString());
        }

        /// <summary>
        ///A test for RemoveParent
        ///</summary>
        [TestMethod]
        public void RemoveParentTest()
        {
            Assert.AreEqual(directoryB.RemoveParent(directoryB), root);
            Assert.AreEqual(fileB.RemoveParent(directoryA), FileSystemPath.Parse("/fileb.txt"));
            Assert.AreEqual(root.RemoveParent(root), root);
            Assert.AreEqual(directoryB.RemoveParent(root), directoryB);
            EAssert.Throws<ArgumentException>(() => fileB.RemoveParent(FileSystemPath.Parse("/nonexistantparent/")));
            EAssert.Throws<ArgumentException>(() => fileB.RemoveParent(FileSystemPath.Parse("/nonexistantparent")));
            EAssert.Throws<ArgumentException>(() => fileB.RemoveParent(FileSystemPath.Parse("/fileb.txt")));
            EAssert.Throws<ArgumentException>(() => fileB.RemoveParent(FileSystemPath.Parse("/directorya")));
        }

        /// <summary>
        ///A test for RemoveChild
        ///</summary>
        [TestMethod]
        public void RemoveChildTest()
        {
            Assert.AreEqual(fileB.RemoveChild(FileSystemPath.Parse("/fileb.txt")), directoryA);
            Assert.AreEqual(directoryB.RemoveChild(FileSystemPath.Parse("/directoryb/")), directoryA);
            Assert.AreEqual(directoryB.RemoveChild(directoryB), root);
            Assert.AreEqual(fileB.RemoveChild(fileB), root);
            EAssert.Throws<ArgumentException>(() => directoryA.RemoveChild(FileSystemPath.Parse("/nonexistantchild")));
            EAssert.Throws<ArgumentException>(() => directoryA.RemoveChild(FileSystemPath.Parse("/directorya")));
        }

        /// <summary>
        ///A test for Parse
        ///</summary>
        [TestMethod]
        public void ParseTest()
        {
            Assert.IsTrue(_paths.All(p => p == FileSystemPath.Parse(p.ToString())));
            EAssert.Throws<ArgumentNullException>(() => FileSystemPath.Parse(null));
            EAssert.Throws<ParseException>(() => FileSystemPath.Parse("thisisnotapath"));
            EAssert.Throws<ParseException>(() => FileSystemPath.Parse("/thisisainvalid//path"));
        }

        /// <summary>
        ///A test for IsRooted
        ///</summary>
        [TestMethod]
        public void IsRootedTest()
        {
            Assert.IsTrue(FileSystemPath.IsRooted("/filea"));
            Assert.IsTrue(FileSystemPath.IsRooted("/directorya/"));
            Assert.IsFalse(FileSystemPath.IsRooted("filea"));
            Assert.IsFalse(FileSystemPath.IsRooted("directorya/"));
            Assert.IsTrue(_paths.All(p => FileSystemPath.IsRooted(p.ToString())));
        }

        /// <summary>
        ///A test for IsParentOf
        ///</summary>
        [TestMethod]
        public void IsParentOfTest()
        {
            Assert.IsTrue(directoryA.IsParentOf(fileB));
            Assert.IsTrue(directoryA.IsParentOf(directoryB));
            Assert.IsTrue(root.IsParentOf(fileA));
            Assert.IsTrue(root.IsParentOf(directoryA));
            Assert.IsTrue(root.IsParentOf(fileB));
            Assert.IsTrue(root.IsParentOf(directoryB));

            Assert.IsFalse(fileB.IsParentOf(directoryA));
            Assert.IsFalse(directoryB.IsParentOf(directoryA));
            Assert.IsFalse(fileA.IsParentOf(root));
            Assert.IsFalse(directoryA.IsParentOf(root));
            Assert.IsFalse(fileB.IsParentOf(root));
            Assert.IsFalse(directoryB.IsParentOf(root));
        }

        /// <summary>
        ///A test for IsChildOf
        ///</summary>
        [TestMethod]
        public void IsChildOfTest()
        {
            Assert.IsTrue(fileB.IsChildOf(directoryA));
            Assert.IsTrue(directoryB.IsChildOf(directoryA));
            Assert.IsTrue(fileA.IsChildOf(root));
            Assert.IsTrue(directoryA.IsChildOf(root));
            Assert.IsTrue(fileB.IsChildOf(root));
            Assert.IsTrue(directoryB.IsChildOf(root));

            Assert.IsFalse(directoryA.IsChildOf(fileB));
            Assert.IsFalse(directoryA.IsChildOf(directoryB));
            Assert.IsFalse(root.IsChildOf(fileA));
            Assert.IsFalse(root.IsChildOf(directoryA));
            Assert.IsFalse(root.IsChildOf(fileB));
            Assert.IsFalse(root.IsChildOf(directoryB));
        }

        /// <summary>
        ///A test for GetExtension
        ///</summary>
        [TestMethod]
        public void GetExtensionTest()
        {
            Assert.AreEqual(fileA.GetExtension(), "");
            Assert.AreEqual(fileB.GetExtension(), ".txt");
            fileC = FileSystemPath.Parse("/directory.txt/filec");
            Assert.AreEqual(fileC.GetExtension(), "");
            EAssert.Throws<ArgumentException>(() => directoryA.GetExtension());
        }

        /// <summary>
        ///A test for GetDirectorySegments
        ///</summary>
        [TestMethod]
        public void GetDirectorySegmentsTest()
        {
            Assert.AreEqual(0, root.GetDirectorySegments().Length);
            Directories
                .Where(d => !d.IsRoot)
                .All(d => d.GetDirectorySegments().Length == d.ParentPath.GetDirectorySegments().Length - 1);
            Files.All(f => f.GetDirectorySegments().Length == f.ParentPath.GetDirectorySegments().Length);
        }


        /// <summary>
        ///A test for CompareTo
        ///</summary>
        [TestMethod]
        public void CompareToTest()
        {
            foreach (var pa in _paths)
                foreach (var pb in _paths)
                    Assert.AreEqual(Math.Sign(pa.CompareTo(pb)), Math.Sign(String.Compare(pa.ToString(), pb.ToString(), StringComparison.Ordinal)));
        }

        /// <summary>
        ///A test for ChangeExtension
        ///</summary>
        [TestMethod]
        public void ChangeExtensionTest()
        {
            foreach (var p in _paths.Where(p => p.IsFile))
                Assert.IsTrue(p.ChangeExtension(".exe").GetExtension() == ".exe");
            EAssert.Throws<ArgumentException>(() => directoryA.ChangeExtension(".exe"));
        }

        /// <summary>
        ///A test for AppendPath
        ///</summary>
        [TestMethod]
        public void AppendPathTest()
        {
            Assert.IsTrue(Directories.All(p => p.AppendPath(root) == p));
            Assert.IsTrue(Directories.All(p => p.AppendPath("") == p));

            var subpath = FileSystemPath.Parse("/dir/file");
            var subpathstr = "dir/file";
            foreach (var p in Directories)
                Assert.IsTrue(p.AppendPath(subpath).ParentPath.ParentPath == p);
            foreach (var p in Directories)
                Assert.IsTrue(p.AppendPath(subpathstr).ParentPath.ParentPath == p);
            foreach (var pa in Directories)
                foreach (var pb in _paths.Where(pb => !pb.IsRoot))
                    Assert.IsTrue(pa.AppendPath(pb).IsChildOf(pa));
            EAssert.Throws<InvalidOperationException>(() => fileA.AppendPath(subpath));
            EAssert.Throws<InvalidOperationException>(() => fileA.AppendPath(subpathstr));
            EAssert.Throws<ArgumentException>(() => directoryA.AppendPath("/rootedpath/"));
        }

        /// <summary>
        ///A test for AppendFile
        ///</summary>
        [TestMethod]
        public void AppendFileTest()
        {
            foreach (var d in Directories)
                Assert.IsTrue(d.AppendFile("file").IsFile);
            foreach (var d in Directories)
                Assert.IsTrue(d.AppendFile("file").EntityName == "file");
            foreach (var d in Directories)
                Assert.IsTrue(d.AppendFile("file").ParentPath == d);
            EAssert.Throws<InvalidOperationException>(() => fileA.AppendFile("file"));
            EAssert.Throws<ArgumentException>(() => directoryA.AppendFile("dir/file"));
        }

        /// <summary>
        ///A test for AppendDirectory
        ///</summary>
        [TestMethod]
        public void AppendDirectoryTest()
        {
            foreach (var d in Directories)
                Assert.IsTrue(d.AppendDirectory("dir").IsDirectory);
            foreach (var d in Directories)
                Assert.IsTrue(d.AppendDirectory("dir").EntityName == "dir");
            foreach (var d in Directories)
                Assert.IsTrue(d.AppendDirectory("dir").ParentPath == d);
            EAssert.Throws<InvalidOperationException>(() => fileA.AppendDirectory("dir"));
            EAssert.Throws<ArgumentException>(() => root.AppendDirectory("dir/dir"));
        }
    }
}