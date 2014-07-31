package com.linkedin.uif.metrics;

/**
 * An enumeration of metric name suffices.
 *
 * @author ynli
 */
public enum MetricNameSuffix {
    MIN_VALUE("MinValue"),
    MAX_VALUE("MaxValue"),
    MEDIAN_VALUE("MedianValue"),
    MEAN_VALUE("MeanValue"),
    STDDEV_VALUE("StdDevValue"),
    MEAN_EVENT_RATE("MeanEventRate"),
    MIN_DURATION("MinDuration"),
    MAX_DURATION("MaxDuration"),
    MEDIAN_DURATION("MedianDuration"),
    MEAN_DURATION("MeanDuration"),
    STDDEV_DURATION("StdDevDuration"),
    NONE("");

    private final String suffix;

    private MetricNameSuffix(String suffix) {
        this.suffix = suffix;
    }

    /**
     * Get a succinct representation of this {@link MetricNameSuffix}.
     *
     * @return succinct representation of this {@link MetricNameSuffix}
     */
    public String getSuffix() {
        return this.suffix;
    }
}
