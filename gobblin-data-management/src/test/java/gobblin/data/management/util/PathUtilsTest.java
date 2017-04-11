/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.data.management.util;

import gobblin.util.PathUtils;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PathUtilsTest {

  @Test public void testMergePaths() throws Exception {
    Path path1 = new Path("/some/path");
    Path path2 = new Path("/path2/file");
    Assert.assertEquals(PathUtils.mergePaths(path1, path2).toString(), "/some/path/path2/file");
  }

  @Test public void testRelativizePath() throws Exception {
    Path prefix = new Path("/prefix/path");
    Path suffix = new Path("suffix/elements");
    Path path1 = new Path(prefix, suffix);

    Assert.assertEquals(PathUtils.relativizePath(path1, prefix), suffix);

    Path path2 = new Path("/unrelated/path/with/more/elements");
    Assert.assertEquals(PathUtils.relativizePath(path2, prefix), path2);

  }

  @Test public void testIsAncestor() throws Exception {
    Path ancestor = new Path("/some/path");
    Assert.assertTrue(PathUtils.isAncestor(ancestor, new Path(ancestor, "more/elements")));
    Assert.assertTrue(PathUtils.isAncestor(ancestor, ancestor));
    Assert.assertFalse(PathUtils.isAncestor(ancestor, new Path("/unrelated/path")));
    Assert.assertFalse(PathUtils.isAncestor(ancestor, new Path("relative/path")));
    Assert.assertFalse(PathUtils.isAncestor(ancestor, ancestor.getParent()));

    Path relativeAncestor = new Path("relative/ancestor");
    Assert.assertTrue(PathUtils.isAncestor(relativeAncestor, new Path(relativeAncestor, "more/elements")));
    Assert.assertTrue(PathUtils.isAncestor(relativeAncestor, relativeAncestor));
    Assert.assertFalse(PathUtils.isAncestor(relativeAncestor, new Path("/unrelated/path")));
    Assert.assertFalse(PathUtils.isAncestor(relativeAncestor, new Path("relative/path")));
    Assert.assertFalse(PathUtils.isAncestor(relativeAncestor, relativeAncestor.getParent()));

  }

  @Test
  public void testIsNotAncestor() throws Exception {
    Path ancestor = new Path("/user/gobblin/hourly");
    Assert.assertFalse(PathUtils.isAncestor(ancestor, new Path("hdfs://clus-nn01.company.com:10000/user/gobblin/")));
  }

  @Test public void testGetPathWithoutSchemeAndAuthority() throws Exception {
    Path schemeAndAuthority = new Path("hdfs://example.hdfs:9000/");
    Path path = new Path("/some/path");
    Path fullPath = new Path(schemeAndAuthority, path);
    Assert.assertTrue(fullPath.toString().startsWith("hdfs"));
    Assert.assertEquals(PathUtils.getPathWithoutSchemeAndAuthority(fullPath), path);
  }

  @Test public void testDeepestNonGlobPath() throws Exception {

    Assert.assertEquals(PathUtils.deepestNonGlobPath(new Path("/path/*")), new Path("/path"));
    Assert.assertEquals(PathUtils.deepestNonGlobPath(new Path("/path/*/*")), new Path("/path"));
    Assert.assertEquals(PathUtils.deepestNonGlobPath(new Path("/path/a?b")), new Path("/path"));
    Assert.assertEquals(PathUtils.deepestNonGlobPath(new Path("/path/*.avro")), new Path("/path"));
    Assert.assertEquals(PathUtils.deepestNonGlobPath(new Path("/path/[abc]")), new Path("/path"));
    Assert.assertEquals(PathUtils.deepestNonGlobPath(new Path("/path/{ab,bc}")), new Path("/path"));
    Assert.assertEquals(PathUtils.deepestNonGlobPath(new Path("/path/*/files")), new Path("/path"));
    Assert.assertEquals(PathUtils.deepestNonGlobPath(new Path("/*")), new Path("/"));

  }

  @Test
  public void testRemoveExtension() throws Exception {

    Path path = PathUtils.removeExtension(new Path("file.txt"), ".txt");
    Assert.assertEquals(path, new Path("file"));

    path = PathUtils.removeExtension(new Path("file.txt"), ".abc");
    Assert.assertEquals(path, new Path("file.txt"));

    path = PathUtils.removeExtension(new Path("file.txt.gpg"), ".txt", ".gpg");
    Assert.assertEquals(path, new Path("file"));

    path = PathUtils.removeExtension(new Path("file.txt.gpg"), ".gpg", ".txt");
    Assert.assertEquals(path, new Path("file"));

    path = PathUtils.removeExtension(new Path("file.txt.gpg"), ".txt");
    Assert.assertEquals(path, new Path("file.gpg"));

    path = PathUtils.removeExtension(new Path("file.txt.gpg"), ".gpg");
    Assert.assertEquals(path, new Path("file.txt"));

    path = PathUtils.removeExtension(new Path("file"), ".txt", ".gpg");
    Assert.assertEquals(path, new Path("file"));

  }

  @Test
  public void testAddExtension() throws Exception {

    Path path = PathUtils.addExtension(new Path("file"), ".txt");
    Assert.assertEquals(path, new Path("file.txt"));

    path = PathUtils.addExtension(new Path("file.txt"), ".abc");
    Assert.assertEquals(path, new Path("file.txt.abc"));

    path = PathUtils.addExtension(new Path("file.txt.gpg"), ".txt", ".gpg");
    Assert.assertEquals(path, new Path("file.txt.gpg.txt.gpg"));

    path = PathUtils.addExtension(new Path("file.txt.gpg"), ".tar.gz");
    Assert.assertEquals(path, new Path("file.txt.gpg.tar.gz"));

  }
}
