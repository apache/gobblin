package com.linkedin.uif.metrics;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;

import com.google.common.collect.Maps;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

/**
 * A class that represents a set of metrics associated with a given name.
 *
 * @author ynli
 */
public class Metrics implements MetricSet {

    /**
     * Enumeration of metric types.
     */
    public enum MetricType {
        COUNTER, METER, GAUGE
    }

    /**
     * Enumeration of metric groups.
     */
    public enum MetricGroup {
        SYSTEM, JOB, TASK
    }

    // Mapping from job ID to metrics set
    private static final ConcurrentMap<String, Metrics> METRICS_MAP =
            Maps.newConcurrentMap();

    private final String jobName;
    private final String jobId;
    private final MetricRegistry metricRegistry = new MetricRegistry();

    public Metrics(String jobName, String jobId) {
        this.jobName = jobName;
        this.jobId = jobId;
    }

    /**
     * Get a {@link Metrics} instance for the given metrics set name.
     *
     * @param jobName job name of this metrics set
     * @param jobId job ID of this metrics set
     * @return {@link Metrics} instance for the given metrics set name
     */
    public static synchronized Metrics get(String jobName, String jobId) {
        METRICS_MAP.putIfAbsent(jobId, new Metrics(jobName, jobId));
        return METRICS_MAP.get(jobId);
    }

    /**
     * Remove the {@link Metrics} instance for the given metrics set name
     *
     * @param name metrics set name
     * @return removed {@link Metrics} instance or <code>null</code> if {@link Metrics}
     *         instance for the given metrics set name is not found
     */
    public static Metrics remove(String name) {
        return METRICS_MAP.remove(name);
    }

    /**
     * Create a metric name.
     *
     * @param group metric group
     * @param id metric ID
     * @param name metric name
     * @return the concatenated metric name
     */
    public static String metricName(MetricGroup group, String id, String name) {
        return MetricRegistry.name(group.name(), id, name);
    }

    /**
     * Check whether metrics collection and reporting are enabled or not.
     *
     * @param properties Configuration properties
     * @return whether metrics collection and reporting are enabled
     */
    public static boolean isEnabled(Properties properties) {
        return Boolean.valueOf(properties.getProperty(
                ConfigurationKeys.METRICS_ENABLED_KEY,
                ConfigurationKeys.DEFAULT_METRICS_ENABLED));
    }

    /**
     * Check whether metrics collection and reporting are enabled or not.
     *
     * @param state a {@link State} object containing configuration properties
     * @return whether metrics collection and reporting are enabled
     */
    public static boolean isEnabled(State state) {
        return Boolean.valueOf(state.getProp(
                ConfigurationKeys.METRICS_ENABLED_KEY,
                ConfigurationKeys.DEFAULT_METRICS_ENABLED));
    }

    /**
     * Get the job name of this metrics set.
     *
     * @return job name of this metrics set
     */
    public String getJobName() {
        return this.jobName;
    }

    /**
     * Get the job ID of this metrics set.
     *
     * @return job ID of this metrics set
     */
    public String getJobId() {
        return this.jobId;
    }

    /**
     * Create a new {@link com.codahale.metrics.Counter}.
     *
     * @param group metric group
     * @param id metric ID
     * @param name metric name
     * @return newly created {@link com.codahale.metrics.Counter}
     */
    public Counter getCounter(MetricGroup group, String id, String name) {
        return metricRegistry.counter(metricName(group, id, name));
    }

    /**
     * Create a new {@link com.codahale.metrics.Counter} with the given name.
     *
     * @param name concatenated metric name
     * @return newly created {@link com.codahale.metrics.Counter}
     */
    public Counter getCounter(String name) {
        return metricRegistry.counter(name);
    }

    /**
     * Get a {@link com.codahale.metrics.Meter}.
     *
     * @param group metric group
     * @param id metric ID
     * @param name metric name
     * @return newly created {@link com.codahale.metrics.Meter}
     */
    public Meter getMeter(MetricGroup group, String id, String name) {
        return metricRegistry.meter(metricName(group, id, name));
    }

    /**
     * Get a {@link com.codahale.metrics.Meter} with the given name.
     *
     * @param name concatenated metric name
     * @return newly created {@link com.codahale.metrics.Meter}
     */
    public Meter getMeter(String name) {
        return metricRegistry.meter(name);
    }

    /**
     * Register a {@link com.codahale.metrics.Gauge}.
     *
     * @param group metric group
     * @param id metric ID
     * @param name metric name
     * @param gauge the {@link com.codahale.metrics.Gauge} to register
     * @param <T> gauge data type
     */
    public <T> Gauge<T> getGauge(MetricGroup group, String id, String name, Gauge<T> gauge) {
        return metricRegistry.register(metricName(group, id, name), gauge);
    }

    /**
     * Register a {@link com.codahale.metrics.Gauge} with the given name.
     *
     * @param name concatenated metric name
     * @param gauge the {@link com.codahale.metrics.Gauge} to register
     * @param <T> gauge data type
     */
    public <T> Gauge<T> getGauge(String name, Gauge<T> gauge) {
        return metricRegistry.register(name, gauge);
    }

    /**
     * Remove the metric object associated with the given name.
     *
     * @param name metric object name
     */
    public void removeMetric(String name) {
        metricRegistry.remove(name);
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return metricRegistry.getMetrics();
    }
}
