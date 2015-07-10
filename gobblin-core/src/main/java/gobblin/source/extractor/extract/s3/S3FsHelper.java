/* (c) 2015 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;
import com.jcraft.jsch.UserInfo;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.source.extractor.filebased.FileBasedHelper;
import gobblin.source.extractor.filebased.FileBasedHelperException;
import gobblin.util.S3Utils;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Connects to an S3 Source in a file-based manner, exposing file-based commands to interface with S3
 * @author ahollenbach@nerdwallet.com
 */
public class S3FsHelper implements FileBasedHelper {
  private static final Logger LOG = LoggerFactory.getLogger(S3FsHelper.class);

  private State state;
  private AmazonS3 s3Client;
  private String s3Bucket;
  private String s3Path;

  /**
   * Creates an S3 filesystem helper. This method creates and stores the Amazon S3 client, as well as fetches the
   * bucket that the source should be looking into.
   * @param state
   */
  public S3FsHelper(State state) {
    this.state = state;

    s3Client = new AmazonS3Client();
    s3Bucket = state.getProp(ConfigurationKeys.S3_SOURCE_BUCKET);
  }

  /**
   * Opens up a connection to S3. Credentials need to be stored in environment variables.
   * <p/>
   * In reality, S3 doesn't have an open connection to maintain - individual actions such as a listing or object
   * content download require a temporary open connection.
   * <p/>
   * In this method, the S3 path is set. This is because the source path can contain a date, which we want to be
   * updated every time we "connect".
   */
  @Override
  public void connect() {
    s3Path = state.getProp(ConfigurationKeys.S3_SOURCE_PATH);

    // Replace the date if needed (if none found, s3Path is unaffected)
    s3Path = S3Utils.checkAndReplaceDate(state, s3Path);
    LOG.info("S3PATH:" + s3Path);
  }

  /**
   * Gets a stream to an object in S3
   * @param objectKey The object key to look up in S3
   */
  public InputStream getFileStream(String objectKey)
      throws FileBasedHelperException {
    // Fetch our object from S3 and build an input stream to read from for each record
    S3Object obj = s3Client.getObject(s3Bucket, objectKey);
    return obj.getObjectContent();
  }

  /**
   * Gets a list of S3 object keys. This method will search respecting the wildcard operator (*)
   * for any "directories" in S3.
   * @param path The full path up until the last delimiter, excluding what would traditionally be respected as the
   *             "filename".
   * @return A list of all S3 object keys that match the path given. Thinking in a filesystem way, this has the
   * potential to return files from multiple folders, due to the wildcard operator.
   *
   * @throws FileBasedHelperException
   */
  @Override
  @SuppressWarnings("unchecked")
  public List<String> ls(String path)
      throws FileBasedHelperException {
    try {
      List<String> objectKeys = recursivelyGetObjectKeys(s3Path);
      if (objectKeys.size() == 0) {
        LOG.warn("S3 bucket/path was empty, no results found.");
        LOG.warn("S3 Bucket: " + s3Bucket);
        LOG.warn("S3 Path: " + s3Path);
      } else {
        LOG.info("Found " + objectKeys.size() + " S3 objects!");
      }
      return objectKeys;
    } catch (Exception e) {
      // TODO better exception finding
      throw new FileBasedHelperException("Issue executing ls command on S3", e);
    }
  }

  /**
   * Recursively gets object keys for a given path. This is recursive due to the nature
   * of replacing all wildcards with any possible file paths
   *
   * @param path A path in S3, potentially with wildcards, but no other placeholders (i.e. date placeholders)
   * @return A list of S3 object keys that match the given S3 path
   */
  private List<String> recursivelyGetObjectKeys(String path) {
    String s3PathDelimiter = state.getProp(ConfigurationKeys.S3_PATH_DELIMITER,
        ConfigurationKeys.DEFAULT_S3_PATH_DELIMITER);

    String[] pathParts = path.split("\\*" + s3PathDelimiter, 2);

    if(pathParts.length > 1) {
      // keep recursing splitting and replacing our * with all folders in the dir
      ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName(s3Bucket)
          .withPrefix(pathParts[0])
          .withDelimiter(s3PathDelimiter);

      ObjectListing objectListing = s3Client.listObjects(listObjectRequest);
      List<String> folders = objectListing.getCommonPrefixes();
      while (objectListing.isTruncated()) {
        objectListing = s3Client.listNextBatchOfObjects(objectListing);
        folders.addAll(objectListing.getCommonPrefixes());
      }

      // For each folder, replace what we have and recurse until we are out of wildcards
      List<String> objectSummaries = Lists.newArrayList();
      for (String folderPath : folders) {
        objectSummaries.addAll(recursivelyGetObjectKeys(folderPath + pathParts[1]));
      }
      return objectSummaries;
    }

    // Otherwise, return whatever our path is
    // Build the request
    ListObjectsRequest listObjectRequest = new ListObjectsRequest().withBucketName(s3Bucket).withPrefix(path);

    // Fetch all the objects in the given bucket/path
    ObjectListing objectListing = s3Client.listObjects(listObjectRequest);
    List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();

    // We have to do this if there are a large number of objects in the given path
    // Create the workunits and add them immediately to reduce memory load
    while (objectListing.isTruncated()) {
      objectListing = s3Client.listNextBatchOfObjects(objectListing);
      objectSummaries.addAll(objectListing.getObjectSummaries());
    }

    return getObjectKeys(objectSummaries);
  }

  /**
   * Given a list of object summaries, gets the object keys
   *
   * @param objectSummaries A list of Amazon object summaries
   * @return The corresponding list of object keys
   */
  private List<String> getObjectKeys(List<S3ObjectSummary> objectSummaries) {
    List<String> objectKeys = Lists.newArrayList();
    for (S3ObjectSummary summary : objectSummaries) {
      objectKeys.add(summary.getKey());
    }
    return objectKeys;
  }

  @Override
  public void close() {
  }

  public String getS3Path() {
    return this.s3Path;
  }
}
