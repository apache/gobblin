package gobblin.metrics.event.sla;

import java.util.Map;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import gobblin.metrics.event.EventSubmitter;

/**
 * A wrapper around the {@link EventSubmitter} which can submit SLA Events.
 */
@Builder
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SlaEventSubmitter {

  private EventSubmitter eventSubmitter;
  private String eventName;
  private String datasetUrn;
  private String partition;
  private String originTimestamp;
  private String upstreamTimestamp;
  private String recordCount;
  private String previousPublishTimestamp;
  private String dedupeStatus;
  private String completenessPercentage;
  @Singular("additionalMetadata") private Map<String, String> additionalMetadata;

  /**
   * Construct an {@link SlaEventSubmitter} by extracting Sla event metadata from the properties. See
   * {@link SlaEventKeys} for keys to set in properties
   *
   * <p>
   * Use {@link SlaEventSubmitter#builder()} to build an {@link SlaEventSubmitter} directly with event metadata.
   * </p>
   *
   * @param submitter used to submit the event
   * @param name of the event
   * @param props reference that contains event metadata
   */
  public SlaEventSubmitter(EventSubmitter submitter, String name , Properties props) {

    this.eventName = name;
    this.eventSubmitter = submitter;
    this.datasetUrn = props.getProperty(SlaEventKeys.DATASET_URN_KEY);
    this.partition = props.getProperty(SlaEventKeys.PARTITION_KEY);
    this.originTimestamp = props.getProperty(SlaEventKeys.ORIGIN_TS_IN_MILLI_SECS_KEY);
    this.upstreamTimestamp = props.getProperty(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY);
    this.completenessPercentage = props.getProperty(SlaEventKeys.COMPLETENESS_PERCENTAGE_KEY);
    this.recordCount = props.getProperty(SlaEventKeys.RECORD_COUNT_KEY);
    this.previousPublishTimestamp = props.getProperty(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY);
    this.dedupeStatus = props.getProperty(SlaEventKeys.DEDUPE_STATUS_KEY);

  }

  /**
   * Submit the sla event by calling {@link SlaEventSubmitter#EventSubmitter#submit()}. If
   * {@link SlaEventSubmitter#eventName}, {@link SlaEventSubmitter#eventSubmitter}, {@link SlaEventSubmitter#datasetUrn}
   * , {@link SlaEventSubmitter#partition} are not available the method is a no-op.
   */
  public void submit() {
    try {

      Preconditions.checkNotNull(eventSubmitter, "EventSubmitter needs to be set");
      Preconditions.checkArgument(NOT_NULL_OR_EMPTY_PREDICATE.apply(eventName), "Eventname is required");
      Preconditions.checkArgument(NOT_NULL_OR_EMPTY_PREDICATE.apply(datasetUrn), "DatasetUrn is required");
      Preconditions.checkArgument(NOT_NULL_OR_EMPTY_PREDICATE.apply(partition), "Partition is required");

      eventSubmitter.submit(eventName, buildEventMap());

    } catch (IllegalArgumentException e) {
      log.info("Required arguments to submit an SLA event is not available. No Sla event will be submitted", e.toString());
    }
  }

  /**
   * Builds an EventMetadata {@link Map} from the {@link #SlaEventSubmitter}. The method filters out metadata by
   * applying {@link #NOT_NULL_OR_EMPTY_PREDICATE}
   *
   */
  private Map<String, String> buildEventMap() {

    Map<String, String> eventMetadataMap = Maps.newHashMap();
    eventMetadataMap.put(withoutPropertiesPrefix(SlaEventKeys.DATASET_URN_KEY), datasetUrn);
    eventMetadataMap.put(withoutPropertiesPrefix(SlaEventKeys.PARTITION_KEY), partition);
    eventMetadataMap.put(withoutPropertiesPrefix(SlaEventKeys.ORIGIN_TS_IN_MILLI_SECS_KEY), originTimestamp);
    eventMetadataMap.put(withoutPropertiesPrefix(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY), upstreamTimestamp);
    eventMetadataMap.put(withoutPropertiesPrefix(SlaEventKeys.COMPLETENESS_PERCENTAGE_KEY), completenessPercentage);
    eventMetadataMap.put(withoutPropertiesPrefix(SlaEventKeys.RECORD_COUNT_KEY), recordCount);
    eventMetadataMap.put(withoutPropertiesPrefix(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY), previousPublishTimestamp);
    eventMetadataMap.put(withoutPropertiesPrefix(SlaEventKeys.DEDUPE_STATUS_KEY), dedupeStatus);

    if (additionalMetadata != null) {
      eventMetadataMap.putAll(additionalMetadata);
    }
    return Maps.newHashMap(Maps.filterValues(eventMetadataMap, NOT_NULL_OR_EMPTY_PREDICATE));
  }

  /**
   * {@link SlaEventKeys} have a prefix of {@link SlaEventKeys#EVENT_GOBBLIN_STATE_PREFIX} to keep properties organized
   * in state. This method removes the prefix before submitting an Sla event.
   */
  private String withoutPropertiesPrefix(String key) {
    return StringUtils.removeStart(key, SlaEventKeys.EVENT_GOBBLIN_STATE_PREFIX);
  }

  /**
   * Predicate that returns false if a string is null or empty. Calls {@link Strings#isNullOrEmpty(String)} internally.
   */
  private static final Predicate<String> NOT_NULL_OR_EMPTY_PREDICATE = new Predicate<String>() {

    @Override
    public boolean apply(String input) {
      return !Strings.isNullOrEmpty(input);
    }
  };
}
