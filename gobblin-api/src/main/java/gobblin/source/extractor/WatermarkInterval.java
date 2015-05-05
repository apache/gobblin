package gobblin.source.extractor;

import gobblin.fork.CopyNotSupportedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

public class WatermarkInterval implements Writable {

  private Watermark lowWatermark;
  private Watermark expectedHighWatermark;
  private Watermark actualHighWatermark;

  // Needed for the Writable interface
  public WatermarkInterval() {
  }

  private WatermarkInterval(Watermark lowWatermark, Watermark expectedHighWatermark) throws CopyNotSupportedException {
    this.lowWatermark = lowWatermark;
    this.expectedHighWatermark = expectedHighWatermark;
    this.actualHighWatermark = lowWatermark.copy();
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
    try {
      this.lowWatermark = (Watermark) Class.forName(Text.readString(in)).newInstance();
      this.lowWatermark.initFromJson(Text.readString(in));
    } catch (InstantiationException e) {
      throw new IOException("Could not properly initialize the low watermark", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Could not properly initialize the low watermark", e);
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not properly initialize the low watermark", e);
    }

    try {
      this.expectedHighWatermark = (Watermark) Class.forName(Text.readString(in)).newInstance();
      this.expectedHighWatermark.initFromJson(Text.readString(in));
    } catch (InstantiationException e) {
      throw new IOException("Could not properly initialize the expected high watermark", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Could not properly initialize the expected high watermark", e);
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not properly initialize the expected high watermark", e);
    }

    try {
      this.actualHighWatermark = (Watermark) Class.forName(Text.readString(in)).newInstance();
      this.actualHighWatermark.initFromJson(Text.readString(in));
    } catch (InstantiationException e) {
      throw new IOException("Could not properly initialize the actual high watermark", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Could not properly initialize the actual high watermark", e);
    } catch (ClassNotFoundException e) {
      throw new IOException("Could not properly initialize the actual high watermark", e);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.lowWatermark.getClass().getName());
    Text.writeString(out, this.lowWatermark.toJson());

    Text.writeString(out, this.expectedHighWatermark.getClass().getName());
    Text.writeString(out, this.expectedHighWatermark.toJson());

    Text.writeString(out, this.actualHighWatermark.getClass().getName());
    Text.writeString(out, this.actualHighWatermark.toJson());
  }
}
