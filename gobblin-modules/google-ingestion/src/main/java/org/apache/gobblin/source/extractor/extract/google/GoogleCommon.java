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
package org.apache.gobblin.source.extractor.extract.google;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Utility class that has static methods for Google services.
 *
 */
public class GoogleCommon {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCommon.class);
  private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
  private static final String JSON_FILE_EXTENSION = ".json";

  private static final FsPermission USER_READ_PERMISSION_ONLY = new FsPermission(FsAction.READ,
                                                                                 FsAction.NONE,
                                                                                 FsAction.NONE);

  public static class CredentialBuilder {
    private final String privateKeyPath;
    private final Collection<String> serviceAccountScopes;
    private String fileSystemUri;
    private String serviceAccountId;
    private String proxyUrl;
    //Port as String type, so that client can easily pass null instead of checking the existence of it.
    //( e.g: state.getProp(key) vs state.contains(key) + state.getPropAsInt(key) )
    private String portStr;


    public CredentialBuilder(String privateKeyPath, Collection<String> serviceAccountScopes) {
      Preconditions.checkArgument(!StringUtils.isEmpty(privateKeyPath), "privateKeyPath is required.");
      Preconditions.checkArgument(serviceAccountScopes != null && !serviceAccountScopes.isEmpty(), "serviceAccountScopes is required.");

      this.privateKeyPath = privateKeyPath;
      this.serviceAccountScopes = ImmutableList.copyOf(serviceAccountScopes);
    }

    public CredentialBuilder fileSystemUri(String fileSystemUri) {
      this.fileSystemUri = fileSystemUri;
      return this;
    }

    public CredentialBuilder serviceAccountId(String serviceAccountId) {
      this.serviceAccountId = serviceAccountId;
      return this;
    }

    public CredentialBuilder proxyUrl(String proxyUrl) {
      this.proxyUrl = proxyUrl;
      return this;
    }

    public CredentialBuilder port(int port) {
      this.portStr = Integer.toString(port);
      return this;
    }

    public CredentialBuilder port(String portStr) {
      this.portStr = portStr;
      return this;
    }

    public Credential build() {
      try {
        HttpTransport transport = newTransport(proxyUrl, portStr);

        if (privateKeyPath.trim().toLowerCase().endsWith(JSON_FILE_EXTENSION)) {
          LOG.info("Getting Google service account credential from JSON");
          return buildCredentialFromJson(privateKeyPath,
                                         Optional.fromNullable(fileSystemUri),
                                         transport,
                                         serviceAccountScopes);
        } else {
          LOG.info("Getting Google service account credential from P12");
          return buildCredentialFromP12(privateKeyPath,
                                        Optional.fromNullable(fileSystemUri),
                                        Optional.fromNullable(serviceAccountId),
                                        transport,
                                        serviceAccountScopes);
        }
      } catch (IOException | GeneralSecurityException e) {
        throw new RuntimeException("Failed to create credential", e);
      }
    }
  }

  /**
   * As Google API only accepts java.io.File for private key, and this method copies private key into local file system.
   * Once Google credential is instantiated, it deletes copied private key file.
   *
   * @param privateKeyPath
   * @param fsUri
   * @param id
   * @param transport
   * @param serviceAccountScopes
   * @return Credential
   * @throws IOException
   * @throws GeneralSecurityException
   */
  private static Credential buildCredentialFromP12(String privateKeyPath,
                                                   Optional<String> fsUri,
                                                   Optional<String> id,
                                                   HttpTransport transport,
                                                   Collection<String> serviceAccountScopes) throws IOException, GeneralSecurityException {
    Preconditions.checkArgument(id.isPresent(), "user id is required.");

    FileSystem fs = getFileSystem(fsUri);
    Path keyPath = getPrivateKey(fs, privateKeyPath);

    final File localCopied = copyToLocal(fs, keyPath);
    localCopied.deleteOnExit();
    try {
      return new GoogleCredential.Builder()
                                 .setTransport(transport)
                                 .setJsonFactory(JSON_FACTORY)
                                 .setServiceAccountId(id.get())
                                 .setServiceAccountPrivateKeyFromP12File(localCopied)
                                 .setServiceAccountScopes(serviceAccountScopes)
                                 .build();
    } finally {
      boolean isDeleted = localCopied.delete();
      if (!isDeleted) {
        throw new RuntimeException(localCopied.getAbsolutePath() + " has not been deleted.");
      }
    }
  }

  /**
   * Before retrieving private key, it makes sure that original private key's permission is read only on the owner.
   * This is a way to ensure to keep private key private.
   * @param fs
   * @param privateKeyPath
   * @return
   * @throws IOException
   */
  private static Path getPrivateKey(FileSystem fs, String privateKeyPath) throws IOException {
    Path keyPath = new Path(privateKeyPath);
    FileStatus fileStatus = fs.getFileStatus(keyPath);
    Preconditions.checkArgument(USER_READ_PERMISSION_ONLY.equals(fileStatus.getPermission()),
                                "Private key file should only have read only permission only on user. " + keyPath);
    return keyPath;
  }

  private static FileSystem getFileSystem(Optional<String> fsUri) throws IOException {
    if (fsUri.isPresent()) {
      return FileSystem.get(URI.create(fsUri.get()), new Configuration());
    }
    return FileSystem.get(new Configuration());
  }

  private static Credential buildCredentialFromJson(String privateKeyPath,
                                                    Optional<String> fsUri,
                                                    HttpTransport transport,
                                                    Collection<String> serviceAccountScopes) throws IOException {
    FileSystem fs = getFileSystem(fsUri);
    Path keyPath = getPrivateKey(fs, privateKeyPath);

    return GoogleCredential.fromStream(fs.open(keyPath),
                                       transport,
                                       JSON_FACTORY)
                           .createScoped(serviceAccountScopes);
  }

  /**
   * Provides HttpTransport. If both proxyUrl and postStr is defined, it provides transport with Proxy.
   * @param proxyUrl Optional.
   * @param portStr Optional. String type for port so that user can easily pass null. (e.g: state.getProp(key))
   * @return
   * @throws NumberFormatException
   * @throws GeneralSecurityException
   * @throws IOException
   */
  public static HttpTransport newTransport(String proxyUrl, String portStr) throws NumberFormatException, GeneralSecurityException, IOException {
    if (!StringUtils.isEmpty(proxyUrl) && !StringUtils.isEmpty(portStr)) {
      return new NetHttpTransport.Builder()
                                 .trustCertificates(GoogleUtils.getCertificateTrustStore())
                                 .setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyUrl, Integer.parseInt(portStr))))
                                 .build();
    }
    return GoogleNetHttpTransport.newTrustedTransport();
  }

  private static File copyToLocal(FileSystem fs, Path keyPath) throws IOException {
    java.nio.file.Path tmpKeyPath = Files.createTempFile(GoogleCommon.class.getSimpleName(), "tmp",
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    File copied = tmpKeyPath.toFile();
    copied.deleteOnExit();

    fs.copyToLocalFile(keyPath, new Path(copied.getAbsolutePath()));
    return copied;
  }

  public static JsonFactory getJsonFactory() {
    return JSON_FACTORY;
  }
}
