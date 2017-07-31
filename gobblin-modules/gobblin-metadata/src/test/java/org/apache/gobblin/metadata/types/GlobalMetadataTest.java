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
package org.apache.gobblin.metadata.types;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;


public class GlobalMetadataTest {
  @Test
  public void testMerge() {
    GlobalMetadata m = new GlobalMetadata();
    m.setFileMetadata("file1", "key1", "val1");
    m.addTransferEncoding("gzip");

    GlobalMetadata m2 = new GlobalMetadata();
    m2.setFileMetadata("file2", "key1", "val2");
    m2.addTransferEncoding("gzip");

    GlobalMetadata merged = new GlobalMetadata();
    merged.addAll(m);
    merged.addAll(m2);

    Assert.assertEquals(merged.getFileMetadata("file1", "key1"), "val1");
    Assert.assertEquals(merged.getFileMetadata("file2", "key1"), "val2");
    List<String> mergedEncoding = merged.getTransferEncoding();
    Assert.assertEquals(mergedEncoding.size(), 1);
    Assert.assertEquals(mergedEncoding.get(0), "gzip");
  }

  @Test
  public void testToJson() throws IOException {
    GlobalMetadata m = new GlobalMetadata();
    m.addTransferEncoding("foo");
    m.addTransferEncoding("bar");

    byte[] utf8 = m.toJsonUtf8();
    String parsed = new String(utf8, StandardCharsets.UTF_8);

    JsonNode root = new ObjectMapper().readTree(parsed);
    Assert.assertTrue(root.isObject());
    Iterator<JsonNode> children = root.getElements();
    int numChildren = 0;
    while (children.hasNext()) {
      children.next();
      numChildren++;
    }

    Assert.assertEquals(numChildren, 3, "expected only 3 child nodes - file, dataset, id");
    Assert.assertEquals(root.get("file").size(), 0, "expected no children in file node");
    Assert.assertTrue(root.get("id").isTextual(), "expected ID to be textual");

    JsonNode transferEncoding = root.get("dataset").get("Transfer-Encoding");
    Assert.assertEquals(transferEncoding.size(), m.getTransferEncoding().size());
    for (int i = 0; i < m.getTransferEncoding().size(); i++) {
      Assert.assertEquals(transferEncoding.get(i).getTextValue(), m.getTransferEncoding().get(i));
    }
  }

  @Test
  public void testFromJson() throws IOException {
    String serialized = "{\"dataset\":{\"Transfer-Encoding\":[\"gzip\",\"aes_rotating\"]},\"file\":{\"part.task_hello-world_1488584636479_1.json.gzip.aes_rotating\":{\"exists\":\"true\"}}}";
    GlobalMetadata md = GlobalMetadata.fromJson(serialized);

    List<String> transferEncoding = md.getTransferEncoding();
    Assert.assertEquals(transferEncoding, ImmutableList.of("gzip", "aes_rotating"));

    Assert.assertEquals(md.getFileMetadata("part.task_hello-world_1488584636479_1.json.gzip.aes_rotating", "exists"), "true");
  }

  @Test
  public void testGetId() {
    GlobalMetadata m1 = buildMetadata();
    GlobalMetadata m2 = buildMetadata();

    Assert.assertEquals(m1.getId(), m2.getId());
    m1.addTransferEncoding("baz");
    Assert.assertNotEquals(m1.getId(), m2.getId());
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testImmutableGlobal() {
    GlobalMetadata md = new GlobalMetadata();
    md.setDatasetUrn("Hello");
    md.markImmutable();

    Assert.assertEquals(md.getDatasetUrn(), "Hello");
    md.setDatasetUrn("World"); // should throw
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testImmutableDataset() {
    GlobalMetadata md = new GlobalMetadata();
    md.setFileMetadata("file1", "key1", "val1");
    md.markImmutable();

    Assert.assertEquals(md.getFileMetadata("file1", "key1"), "val1");
    md.setFileMetadata("file1", "key1", "val2"); // should throw

  }
  protected GlobalMetadata buildMetadata() {
    GlobalMetadata m = new GlobalMetadata();
    m.addTransferEncoding("foo");
    m.addTransferEncoding("bar");

    return m;
  }
}
