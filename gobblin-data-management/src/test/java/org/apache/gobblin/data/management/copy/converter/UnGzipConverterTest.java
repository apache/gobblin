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
package org.apache.gobblin.data.management.copy.converter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import joptsimple.internal.Strings;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.data.management.copy.CopyableFileUtils;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;


public class UnGzipConverterTest {

  @DataProvider(name = "fileDataProvider")
  public static Object[][] fileDataProvider() {
    // {filePath, expectedText}
    return new Object[][] { { "unGzipConverterTest/archived.tar.gz", "text" }, { "unGzipConverterTest/archived.tgz", "text" } };
  }

  @Test(dataProvider = "fileDataProvider")
  public void testGz(final String filePath, final String expectedText) throws Exception {

    UnGzipConverter converter = new UnGzipConverter();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    String fullPath = getClass().getClassLoader().getResource(filePath).getFile();
    FileAwareInputStream fileAwareInputStream =
        new FileAwareInputStream(CopyableFileUtils.getTestCopyableFile(filePath), fs.open(new Path(fullPath)));

    Iterable<FileAwareInputStream> iterable =
        converter.convertRecord("outputSchema", fileAwareInputStream, new WorkUnitState());

    String actual = readGzipStreamAsString(Iterables.getFirst(iterable, null).getInputStream());
    Assert.assertEquals(actual.trim(), expectedText);

  }

  @Test
  public void testExtensionStripping() throws DataConversionException, IOException {
    List<String> helloWorldFiles = ImmutableList.of("helloworld.txt.gzip", "helloworld.txt.gz");
    UnGzipConverter converter = new UnGzipConverter();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    for (String fileName: helloWorldFiles) {
      String filePath = "unGzipConverterTest/" + fileName;
      String fullPath = getClass().getClassLoader().getResource(filePath).getFile();

      FileAwareInputStream fileAwareInputStream =
          new FileAwareInputStream(CopyableFileUtils.getTestCopyableFile(filePath, "/tmp/" + fileName, null, null),
              fs.open(new Path(fullPath)));

      Iterable<FileAwareInputStream> iterable = converter.convertRecord("outputSchema", fileAwareInputStream, new WorkUnitState());
      FileAwareInputStream out = iterable.iterator().next();

      Assert.assertEquals(out.getFile().getDestination().getName(), "helloworld.txt");
      String contents = IOUtils.toString(out.getInputStream(), StandardCharsets.UTF_8);

      Assert.assertEquals(contents, "helloworld\n");
    }

  }

  private static String readGzipStreamAsString(InputStream is) throws Exception {
    TarArchiveInputStream tarIn = new TarArchiveInputStream(is);
    try {
      TarArchiveEntry tarEntry;
      while ((tarEntry = tarIn.getNextTarEntry()) != null) {
        if (tarEntry.isFile() && tarEntry.getName().endsWith(".txt")) {
          return IOUtils.toString(tarIn, "UTF-8");
        }
      }
    } finally {
      tarIn.close();
    }

    return Strings.EMPTY;
  }
}
