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
package org.apache.gobblin.yarn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.Resource;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Utility class
 */
public class GobblinYarnTestUtils {
  /**
   * A utility method for generating a {@link org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem} instance
   * that can return a delegation token on {@link org.apache.hadoop.fs.FileSystem#getDelegationToken(String)}.
   *
   * @param service
   * @return
   * @throws IOException
   */
  public static FileSystemTestHelper.MockFileSystem createFileSystemForServiceName(final String service)
      throws IOException {
    FileSystemTestHelper.MockFileSystem mockFs = new FileSystemTestHelper.MockFileSystem();
    Mockito.when(mockFs.getCanonicalServiceName()).thenReturn(service);
    Mockito.when(mockFs.getDelegationToken(Mockito.any(String.class))).thenAnswer(new Answer<Token<?>>() {
      int unique = 0;
      @Override
      public Token<?> answer(InvocationOnMock invocation) throws Throwable {
        Token<?> token = new Token<TokenIdentifier>();
        token.setService(new Text(service));
        // use unique value so when we restore from token storage, we can
        // tell if it's really the same token
        token.setKind(new Text("token" + unique++));
        return token;
      }
    });
    return mockFs;
  }

  /**
   * Writes a token file to a given path.
   * @param path
   * @param serviceName
   * @throws IOException
   */
  public static void createTokenFileForService(Path path, String serviceName)
      throws IOException {
    FileSystem fileSystem = createFileSystemForServiceName(serviceName);
    Token<?> token = fileSystem.getDelegationToken(serviceName);
    Credentials credentials = new Credentials();
    credentials.addToken(token.getService(), token);
    credentials.writeTokenStorageFile(path, new Configuration());
  }

  public static YarnContainerRequestBundle createYarnContainerRequest(int n, Resource resource) {
    YarnContainerRequestBundle yarnContainerRequestBundle = new YarnContainerRequestBundle();
    yarnContainerRequestBundle.add("GobblinKafkaStreaming", n, resource);
    return yarnContainerRequestBundle;
  }
}