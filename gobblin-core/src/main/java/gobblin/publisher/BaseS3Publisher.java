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

package gobblin.publisher;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

/**
 * An implementation of {@link BaseDataPublisher} that publishes the data from the writer
 * to S3.
 *
 * <p>
 *
 * The user must provide a getBucketAndKey method which returns the S3 bucket and key to post the data
 * to. The publisher iterates through all tasks and appends files with the exact same BucketAndKey.
 * If the file size exceeds 4GB or after all the data has been appended, the data is published to S3.
 * The files written by each task are specified by {@link ConfigurationKeys#WRITER_FINAL_OUTPUT_PATH}.
 *
 * @author akshay@nerdwallet.com
 */
public abstract class BaseS3Publisher extends BaseDataPublisher {
  protected static final int DEFAULT_S3_PARTITIONS = 10;
  protected final int s3Partitions;

  private static final Logger LOG = LoggerFactory.getLogger(BaseS3Publisher.class);

  public BaseS3Publisher(State state) {
    super(state);
    s3Partitions = state.getPropAsInt(ConfigurationKeys.S3_PARTITIONS, DEFAULT_S3_PARTITIONS);
  }

  @Override
  public abstract void publishData(Collection<? extends WorkUnitState> states) throws IOException;

  @Override
  public abstract void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException;

  protected void sendS3Data(BucketAndKey bk, InputStream is, long contentLength) throws IOException {
    // get config parameters
    String awsAccessKey = this.getState().getProp(ConfigurationKeys.AWS_ACCESS_KEY);
    String awsSecretKey = this.getState().getProp(ConfigurationKeys.AWS_SECRET_KEY);
    String bucket = bk.getBucket();
    String key = bk.getKey() + this.getState().getProp(ConfigurationKeys.DATA_PUBLISHER_FILE_EXTENSION, ".txt");

    AmazonS3Client s3Client;
    LOG.info("Attempting to connect to amazon");
    if (awsAccessKey == null || awsSecretKey == null) {
      s3Client = new AmazonS3Client();
    } else {
      s3Client = new AmazonS3Client(new BasicAWSCredentials(awsAccessKey, awsSecretKey));
    }
    s3Client.setRegion(Region.getRegion(Regions.US_EAST_1));
    LOG.info("Established connection to amazon");

    // add content length to send along to amazon
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(contentLength);

    LOG.info("Sending data to amazon with content length = " + contentLength);
    s3Client.putObject(bucket, key, is, metadata);
    s3Client.shutdown();
    LOG.info("Put <" + key + "> to s3 bucket <" + bucket + ">");
  }

  protected static class BucketAndKey {
    private String bucket;
    private String key;

    public BucketAndKey(String b, String k) {
      bucket = b;
      key = k;
    }

    public String getBucket() {
      return bucket;
    }

    public String getKey() {
      return key;
    }

    @Override
    public boolean equals(Object bk) {
      if (bk == null) {
        return false;
      }
      if (bk instanceof BucketAndKey) {
        BucketAndKey bkCast = (BucketAndKey) bk;
        return this.bucket.equals(bkCast.getBucket()) && this.key.equals(bkCast.getKey());
      } else {
        return false;
      }
    }
  }
}
