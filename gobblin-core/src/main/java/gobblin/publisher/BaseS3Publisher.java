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
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
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
  private static final long PART_SIZE = 5 * 1024 * 1024; // 500 mb chunks to s3

  public BaseS3Publisher(State state) {
    super(state);
    s3Partitions = state.getPropAsInt(ConfigurationKeys.S3_PARTITIONS, DEFAULT_S3_PARTITIONS);
  }

  @Override
  public abstract void publishData(Collection<? extends WorkUnitState> states) throws IOException;

  @Override
  public abstract void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException;

  protected void sendS3Data(int branch, BucketAndKey bk, List<String> files) throws IOException {
    if ((files = preprocessFiles(branch, files)).size() == 0) {
      return;
    }
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
      int i = 1;
      for (String file : files) {
        Path filePath = new Path(file);

        // Get the input stream and content length
        long contentLength = fss.get(branch).getFileStatus(filePath).getLen();
        LOG.info("Attempting to send file " + file + " to s3 with content length " + contentLength);
        //InputStream is = fss.get(branch).open(filePath);

        long filePosition = 0;
        while (filePosition < contentLength) {
          // Last part can be less than PART_SIZE. Adjust partSize.
          long partSize = Math.min(PART_SIZE, (contentLength - filePosition));
          // if the last part will be smaller than PART_SIZE, send it along with this part
          if (partSize + filePosition + PART_SIZE >= contentLength) {
            partSize += (contentLength - filePosition - partSize);
          }

          LOG.info("Sending part to s3 with part number " + i + " and file position: " + filePosition);

          // Create request to upload a part.
          UploadPartRequest uploadRequest = new UploadPartRequest()
                  .withBucketName(bk.getBucket()).withKey(bk.getKey())
                  .withUploadId(initResponse.getUploadId()).withPartNumber(i)
                  .withFileOffset(filePosition)
                  //.withInputStream(is)
                  .withFile(new File(file))
                  .withPartSize(partSize);

          // Upload part and add response to our list.
          partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());

          LOG.info("uploadRequest.getFileOffset() = " + uploadRequest.getFileOffset());
          LOG.info("uploadRequest.getPartSize() = " + uploadRequest.getPartSize());
          filePosition += uploadRequest.getPartSize();
          i++;
        }
        LOG.info("Finished publishing file " + file);
      }
      LOG.info("Total parts after publishing all files: " + i);
      // Step 3: Complete.
      CompleteMultipartUploadRequest compRequest = new
              CompleteMultipartUploadRequest(bk.getBucket(),
              bk.getKey(),
              initResponse.getUploadId(),
              partETags);

      s3Client.completeMultipartUpload(compRequest);
    } catch (Exception e) {
      LOG.error("Error publishing to S3:\n" + ExceptionUtils.getStackTrace(e));
      s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
              bk.getBucket(), bk.getKey(), initResponse.getUploadId()));
      return;
    }
    LOG.info("Finished publishing " + bk.toString() + "to s3");
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

  /**
   * Takes a list of files and batches them such that every file size in the resulting list
   * is at least PART_SIZE. This is because in the multipart uploader to s3, all file data must
   * be at least 5 mb except the last one. Thus, if a given file is smaller than 5 mb, it will walk down
   * the linked list and try to append the next file with non-zero length to the end of this file. It
   * will then remove the appended file from the list. Implicitly, all 0 length files will be removed from
   * the final list.
   * @param branch the writer branch we are currently processing
   * @param files the original list of file names
   * @return a modified list of files names which is a subset of files
   */
  private List<String> preprocessFiles(int branch, List<String> files) throws IOException {
    if (files.size() == 0) {
      return files;
    }

    List<String> updatedFiles = new LinkedList<String>();

    Collections.sort(files, new FileComparator());
    Collections.reverse(files);
    for (int i=0; i<files.size();) {
      String fname = files.get(i);
      // need to concatenate
      File f = new File(fname);
      if (f.length() == 0) {
        return updatedFiles;
      }
      updatedFiles.add(fname);
      i++;
      while (f.length() < PART_SIZE) {
        if (i < files.size()) {
          File g = new File(files.get(i));
          if (g.length() == 0) {
            return updatedFiles;
          }
          doAppend(f, g);
          i++;
        }
      }
    }
    return updatedFiles;
  }

  private void doAppend(File f, File g) throws IOException {
    OutputStream out = null;
    InputStream in = null;
    byte[] buf = new byte[1024];
    int b = 0;
    try {
      out = new FileOutputStream(f, true);
      in = new FileInputStream(g);
      while ((b = in.read(buf)) >= 0) {
        out.write(buf, 0, b);
        out.flush();
      }
    } finally {
      if (out != null) {
        out.close();
      }
      if (in != null) {
        in.close();
      }
    }
  }

  private class FileComparator implements Comparator<String> {

    /**
     * Compares its two arguments for order.  Returns a negative integer,
     * zero, or a positive integer as the first argument is less than, equal
     * to, or greater than the second.<p>
     * <p/>
     * In the foregoing description, the notation
     * <tt>sgn(</tt><i>expression</i><tt>)</tt> designates the mathematical
     * <i>signum</i> function, which is defined to return one of <tt>-1</tt>,
     * <tt>0</tt>, or <tt>1</tt> according to whether the value of
     * <i>expression</i> is negative, zero or positive.<p>
     * <p/>
     * The implementor must ensure that <tt>sgn(compare(x, y)) ==
     * -sgn(compare(y, x))</tt> for all <tt>x</tt> and <tt>y</tt>.  (This
     * implies that <tt>compare(x, y)</tt> must throw an exception if and only
     * if <tt>compare(y, x)</tt> throws an exception.)<p>
     * <p/>
     * The implementor must also ensure that the relation is transitive:
     * <tt>((compare(x, y)&gt;0) &amp;&amp; (compare(y, z)&gt;0))</tt> implies
     * <tt>compare(x, z)&gt;0</tt>.<p>
     * <p/>
     * Finally, the implementor must ensure that <tt>compare(x, y)==0</tt>
     * implies that <tt>sgn(compare(x, z))==sgn(compare(y, z))</tt> for all
     * <tt>z</tt>.<p>
     * <p/>
     * It is generally the case, but <i>not</i> strictly required that
     * <tt>(compare(x, y)==0) == (x.equals(y))</tt>.  Generally speaking,
     * any comparator that violates this condition should clearly indicate
     * this fact.  The recommended language is "Note: this comparator
     * imposes orderings that are inconsistent with equals."
     *
     * @param o1 the first object to be compared.
     * @param o2 the second object to be compared.
     * @return a negative integer, zero, or a positive integer as the
     * first argument is less than, equal to, or greater than the
     * second.
     * @throws NullPointerException if an argument is null and this
     *                              comparator does not permit null arguments
     * @throws ClassCastException   if the arguments' types prevent them from
     *                              being compared by this comparator.
     */
    @Override
    public int compare(String o1, String o2) {
      File f1 = new File(o1);
      File f2 = new File(o2);
      if (f1.length() < f2.length()) {
        return -1;
      } else if (f1.length() > f2.length()) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
