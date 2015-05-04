package gobblin.source.extractor;

import gobblin.fork.Copyable;

import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include=JsonTypeInfo.As.PROPERTY, property="@class")
public interface Watermark extends Comparable<Watermark>, Copyable<Watermark> {

  public void increment(Object record);
}
