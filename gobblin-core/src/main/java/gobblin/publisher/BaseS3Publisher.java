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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
  private static final long PART_SIZE = 50 * 1024 * 1024; // 50 mb chunks to s3

  public BaseS3Publisher(State state) {
    super(state);
    s3Partitions = state.getPropAsInt(ConfigurationKeys.S3_PARTITIONS, DEFAULT_S3_PARTITIONS);
  }

  @Override
  public abstract void publishData(Collection<? extends WorkUnitState> states) throws IOException;

  @Override
  public abstract void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException;

  protected void sendS3Data(int branch, BucketAndKey bk, List<String> files) throws IOException {
    AmazonS3 s3Client = new AmazonS3Client();

    // Create a list of UploadPartResponse objects. You get one of these for
    // each part upload.
    List<PartETag> partETags = new ArrayList<PartETag>();

    // Step 1: Initialize.
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
            bk.getBucket(), bk.getKey());
    InitiateMultipartUploadResult initResponse =
            s3Client.initiateMultipartUpload(initRequest);
    try {
      for (String file : files) {
        Path filePath = new Path(file);

        // Get the input stream and content length
        long contentLength = fss.get(branch).getFileStatus(filePath).getLen();
        InputStream is = fss.get(branch).open(filePath);

        long filePosition = 0;
        for (int i = 1; filePosition < contentLength; i++) {
          // Last part can be less than 5 MB. Adjust part size.
          long partSize = Math.min(PART_SIZE, (contentLength - filePosition));

          // Create request to upload a part.
          UploadPartRequest uploadRequest = new UploadPartRequest()
                  .withBucketName(bk.getBucket()).withKey(bk.getKey())
                  .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                  .withFileOffset(filePosition)
                  .withInputStream(is)
                  .withPartSize(partSize);

          // Upload part and add response to our list.
          partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());

          filePosition += partSize;
        }
      }
      // Step 3: Complete.
      CompleteMultipartUploadRequest compRequest = new
              CompleteMultipartUploadRequest(bk.getBucket(),
              bk.getKey(),
              initResponse.getUploadId(),
              partETags);

      s3Client.completeMultipartUpload(compRequest);
    } catch (Exception e) {
      s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
              bk.getBucket(), bk.getKey(), initResponse.getUploadId()));
    }
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

    @Override
    public String toString() {
      return String.format("{Bucket: %s, Key: %s}", bucket, key);
    }
  }
}
