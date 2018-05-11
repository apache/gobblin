package org.apache.gobblin.service.modules.dataset;

public class HdfsDatasetDescriptorImpl implements HdfsDatasetDescriptor {
  private final String path;
  private final String datasetUrn;
  private final String format;
  private final String description;

  public HdfsDatasetDescriptorImpl(String path, String datasetUrn, String format, String description) {
    this.path = path;
    this.datasetUrn = datasetUrn;
    this.format = format;
    this.description = description;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public String getFormat() {
    return format;
  }

  @Override
  public String getPlatform() {
    return "hdfs";
  }

  @Override
  public String getType() {
    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String getDatasetUrn() {
    return datasetUrn;
  }

  /**
   * A {@link HdfsDatasetDescriptor} is compatible with another {@link DatasetDescriptor} iff they have identical
   * platform, type, path, and format.
   * @return true if this {@link HdfsDatasetDescriptor} is compatibe with another {@link DatasetDescriptor}.
   */
  @Override
  public boolean isCompatibleWith(DatasetDescriptor o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if(this.getPlatform() != o.getPlatform() || this.getType() != o.getType()) {
      return false;
    }

    HdfsDatasetDescriptor other = (HdfsDatasetDescriptor) o;
    return this.getPath() == other.getPath() && this.getFormat() == other.getFormat();
  }

  public static class Builder {
    private String path;
    private String datasetUrn;
    private String format;
    private String description = "";

    public HdfsDatasetDescriptor build() {
      return new HdfsDatasetDescriptorImpl(path, datasetUrn, format, description);
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setDatasetUrn(String datasetUrn) {
      this.datasetUrn = datasetUrn;
      return this;
    }

    public Builder setFormat(String format) {
      this.format = format;
      return this;
    }

    public Builder setDescription(String description) {
      this.description = description;
      return this;
    }
  }
}
