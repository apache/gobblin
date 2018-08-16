package org.apache.gobblin.dataset;

import lombok.Getter;


public class PartitionDescriptor extends Descriptor {
  @Getter
  private final DatasetDescriptor dataset;

  public PartitionDescriptor(String name, DatasetDescriptor dataset) {
    super(name);
    this.dataset = dataset;
  }

  @Override
  public PartitionDescriptor copy() {
    return new PartitionDescriptor(getName(), dataset);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionDescriptor that = (PartitionDescriptor) o;
    return dataset.equals(that.dataset) && getName().equals(that.getName());
  }

  @Override
  public int hashCode() {
    int result = dataset.hashCode();
    result = 31 * result + getName().hashCode();
    return result;
  }
}
