package gobblin.publisher;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.util.ForkOperatorUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by akshaynanavati on 5/3/15.
 */
public abstract class BaseS3Publisher extends BaseDataPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(BaseS3Publisher.class);

  // Cap the S3 file size to 4 GB
  private static final long MAX_S3_FILE_SIZE = 4L * 1000000000L;
  // Maps keys to content length
  private final Map<BucketAndKey, Long> contentLengths = new HashMap<BucketAndKey, Long>();
  // Maps keys to respective input streams
  private final Map<BucketAndKey, InputStream> data = new HashMap<BucketAndKey, InputStream>();
  // If the data corresponding to a given key is too big and needs to be split into
  // two keys, this will append a unique number at the end of the key
  private final Map<BucketAndKey, Integer> fragments = new HashMap<BucketAndKey, Integer>();

  public BaseS3Publisher(State state) {
    super(state);
  }

  /**
   * Publishes the data and raises an exception if it can't.
   *
   * @param tasks
   */
  @Override
  public void publishData(Collection<? extends WorkUnitState> tasks) throws IOException {
    for (WorkUnitState task : tasks) {
      for (int i = 0; i < numBranches; i++) {
        BucketAndKey bk = getBucketAndKey(task, i);
        Long contentLength = contentLengths.get(bk);
        if (contentLength == null) {
          contentLength = 0L;
        }
        if (contentLength >= MAX_S3_FILE_SIZE) {
          LOG.info(String.format("Content length <" + contentLength + "> exceeds max size <" + MAX_S3_FILE_SIZE + ">"));
          InputStream is = data.get(bk);
          sendS3Data(bk);
          contentLengths.put(bk, 0L);
          data.put(bk, null);
        }
        String writerFilePathKey =
                ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, numBranches, i);
        if (!task.contains(writerFilePathKey)) {
          // Skip this branch as it does not have data output
          continue;
        }
        Path writerFile = new Path(task.getProp(ConfigurationKeys.WRITER_FINAL_OUTPUT_PATH));
        contentLength += this.fss.get(i).getFileStatus(writerFile).getLen();
        contentLengths.put(bk, contentLength);
        InputStream is = data.get(bk);
        if (is == null) {
          data.put(bk, this.fss.get(i).open(writerFile));
        } else {
          data.put(bk, new SequenceInputStream(this.fss.get(i).open(writerFile), is));
        }
        LOG.info("<" + writerFile + "> has been appended to s3 stream");
      }
    }
    for (BucketAndKey k : data.keySet()) {
      sendS3Data(k);
    }
  }

  /**
   * Returns true if it successfully publishes the metadata, false otherwise. Examples of publishing metadata include
   * writing offset files, checkpoint files, etc.
   *
   * @param tasks
   */
  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> tasks) throws IOException {
    // do nothing
  }

  protected abstract BucketAndKey getBucketAndKey(WorkUnitState task, int branch);

  private void sendS3Data(BucketAndKey bk) throws IOException {
    // get config parameters
    String awsAccessKey = this.getState().getProp(ConfigurationKeys.AWS_ACCESS_KEY);
    String awsSecretKey = this.getState().getProp(ConfigurationKeys.AWS_SECRET_KEY);
    String bucket = bk.getBucket();
    Integer fragment = fragments.get(bk);
    if (fragment == null) {
      fragment = 0;
      fragments.put(bk, 1);
    } else {
      fragments.put(bk, fragment + 1);
    }
    String key = bk.getKey() + "-" + fragment;
    long contentLength = contentLengths.get(bk);
    InputStream is = data.get(bk);

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
