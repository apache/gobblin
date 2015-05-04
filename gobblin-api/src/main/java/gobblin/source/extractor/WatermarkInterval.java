package gobblin.source.extractor;

import gobblin.fork.CopyNotSupportedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Preconditions;

public class WatermarkInterval implements Writable {

  private Watermark lowWatermark;
  private Watermark expectedHighWatermark;
  private Watermark actualHighWatermark;

  private ObjectMapper objectMapper;

  // Needed for the Writable interface
  public WatermarkInterval() {
  }

  private WatermarkInterval(Watermark lowWatermark, Watermark expectedHighWatermark) throws CopyNotSupportedException {
    this.lowWatermark = lowWatermark;
    this.expectedHighWatermark = expectedHighWatermark;
    this.actualHighWatermark = lowWatermark.copy();

    this.objectMapper = new ObjectMapper();
  }

  public static class Builder {

    private Watermark lowWatermark;
    private Watermark expectedHighWatermark;

    public Builder withLowWatermark(Watermark lowWatermark) {
      this.lowWatermark = lowWatermark;
      return this;
    }

    public Builder withExpectedHighWatermark(Watermark expectedHighWatermark) {
      this.expectedHighWatermark = expectedHighWatermark;
      return this;
    }

    public WatermarkInterval build() throws CopyNotSupportedException {
      Preconditions.checkNotNull(this.lowWatermark, "Must specify a low watermark");
      Preconditions.checkNotNull(this.expectedHighWatermark, "Must specify an expected high watermark");

      return new WatermarkInterval(this.lowWatermark, this.expectedHighWatermark);
    }
  }

  public void increment(Object record) {
    this.actualHighWatermark.increment(record);
  }

  public boolean isTargetReached() {
    return this.actualHighWatermark.equals(this.expectedHighWatermark);
  }

  public Watermark getLowWatermark() {
    return this.lowWatermark;
  }

  public Watermark getExpectedHighWatermark() {
    return this.expectedHighWatermark;
  }

  public Watermark getActualHighWatermark() {
    return this.actualHighWatermark;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.lowWatermark = this.objectMapper.readValue(Text.readString(in), Watermark.class);
    this.expectedHighWatermark = this.objectMapper.readValue(Text.readString(in), Watermark.class);
    this.actualHighWatermark = this.objectMapper.readValue(Text.readString(in), Watermark.class);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.objectMapper.writeValueAsString(this.lowWatermark));
    Text.writeString(out, this.objectMapper.writeValueAsString(this.expectedHighWatermark));
    Text.writeString(out, this.objectMapper.writeValueAsString(this.actualHighWatermark));
  }
}
