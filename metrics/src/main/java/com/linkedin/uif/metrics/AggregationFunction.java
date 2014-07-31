package com.linkedin.uif.metrics;

/**
 * An enumeration of aggregation functions on metrics.
 *
 * @author ynli
 */
public enum AggregationFunction {
    COUNT, MIN, MAX, MEAN, MODE, MEDIAN, DISTINCT, DERIVATIVE,
    SUM, STDDEV, FIRST, LAST, DIFFERENCE, TOP, BOTTOM
}
