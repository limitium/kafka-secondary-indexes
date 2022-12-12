package org.apache.kafka.streams.state.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.*;

public class StateStoreMetrics2 {
    private static final String AVG_DESCRIPTION_PREFIX = "The average ";
    private static final String MAX_DESCRIPTION_PREFIX = "The maximum ";
    private static final String LATENCY_DESCRIPTION = "latency of ";
    private static final String AVG_LATENCY_DESCRIPTION_PREFIX = AVG_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;
    private static final String MAX_LATENCY_DESCRIPTION_PREFIX = MAX_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;

    private static final String REBUILD_UNIQ_INDEX = "rebuild-uniq-indexes";
    private static final String REBUILD_UNIQ_INDEX_DESCRIPTION = "rebuild secondary uniq indexes";
    private static final String REBUILD_UNIQ_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + REBUILD_UNIQ_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String REBUILD_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + REBUILD_UNIQ_INDEX_DESCRIPTION;
    private static final String REBUILD_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + REBUILD_UNIQ_INDEX_DESCRIPTION;

    private static final String LOOKUP_UNIQ_INDEX = "lookup-uniq-index";
    private static final String LOOKUP_UNIQ_INDEX_DESCRIPTION = "lookup secondary uniq index";
    private static final String LOOKUP_UNIQ_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + LOOKUP_UNIQ_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String LOOKUP_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + LOOKUP_UNIQ_INDEX_DESCRIPTION;
    private static final String LOOKUP_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + LOOKUP_UNIQ_INDEX_DESCRIPTION;

    private static final String UPDATE_UNIQ_INDEX = "update-uniq-index";
    private static final String UPDATE_UNIQ_INDEX_DESCRIPTION = "update secondary uniq index";
    private static final String UPDATE_UNIQ_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + UPDATE_UNIQ_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String UPDATE_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + UPDATE_UNIQ_INDEX_DESCRIPTION;
    private static final String UPDATE_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + UPDATE_UNIQ_INDEX_DESCRIPTION;


    private static final String REMOVE_UNIQ_INDEX = "remove-uniq-index";
    private static final String REMOVE_UNIQ_INDEX_DESCRIPTION = "remove secondary uniq index";
    private static final String REMOVE_UNIQ_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + REMOVE_UNIQ_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String REMOVE_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + REMOVE_UNIQ_INDEX_DESCRIPTION;
    private static final String REMOVE_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + REMOVE_UNIQ_INDEX_DESCRIPTION;



    private static final String REBUILD_NON_UNIQ_INDEX = "rebuild-uniq-indexes";
    private static final String REBUILD_NON_UNIQ_INDEX_DESCRIPTION = "rebuild secondary uniq indexes";
    private static final String REBUILD_NON_UNIQ_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + REBUILD_NON_UNIQ_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String REBUILD_NON_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + REBUILD_NON_UNIQ_INDEX_DESCRIPTION;
    private static final String REBUILD_NON_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + REBUILD_NON_UNIQ_INDEX_DESCRIPTION;

    private static final String LOOKUP_NON_UNIQ_INDEX = "lookup-uniq-index";
    private static final String LOOKUP_NON_UNIQ_INDEX_DESCRIPTION = "lookup secondary uniq index";
    private static final String LOOKUP_NON_UNIQ_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + LOOKUP_NON_UNIQ_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String LOOKUP_NON_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + LOOKUP_NON_UNIQ_INDEX_DESCRIPTION;
    private static final String LOOKUP_NON_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + LOOKUP_NON_UNIQ_INDEX_DESCRIPTION;

    private static final String UPDATE_NON_UNIQ_INDEX = "update-uniq-index";
    private static final String UPDATE_NON_UNIQ_INDEX_DESCRIPTION = "update secondary uniq index";
    private static final String UPDATE_NON_UNIQ_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + UPDATE_NON_UNIQ_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String UPDATE_NON_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + UPDATE_NON_UNIQ_INDEX_DESCRIPTION;
    private static final String UPDATE_NON_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + UPDATE_NON_UNIQ_INDEX_DESCRIPTION;


    private static final String REMOVE_NON_UNIQ_INDEX = "remove-uniq-index";
    private static final String REMOVE_NON_UNIQ_INDEX_DESCRIPTION = "remove secondary uniq index";
    private static final String REMOVE_NON_UNIQ_INDEX_RATE_DESCRIPTION = RATE_DESCRIPTION_PREFIX + REMOVE_NON_UNIQ_INDEX_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
    private static final String REMOVE_NON_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + REMOVE_NON_UNIQ_INDEX_DESCRIPTION;
    private static final String REMOVE_NON_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + REMOVE_NON_UNIQ_INDEX_DESCRIPTION;

    public static Sensor restoreUniqIndexSensor(final String taskId,
                                                final String storeType,
                                                final String storeName,
                                                final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                REBUILD_UNIQ_INDEX,
                REBUILD_UNIQ_INDEX_RATE_DESCRIPTION,
                REBUILD_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION,
                REBUILD_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    public static Sensor lookupUniqIndexSensor(final String taskId,
                                               final String storeType,
                                               final String storeName,
                                               final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                LOOKUP_UNIQ_INDEX,
                LOOKUP_UNIQ_INDEX_RATE_DESCRIPTION,
                LOOKUP_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION,
                LOOKUP_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    public static Sensor updateUniqIndexSensor(final String taskId,
                                               final String storeType,
                                               final String storeName,
                                               final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                UPDATE_UNIQ_INDEX,
                UPDATE_UNIQ_INDEX_RATE_DESCRIPTION,
                UPDATE_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION,
                UPDATE_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    public static Sensor removeUniqIndexSensor(final String taskId,
                                               final String storeType,
                                               final String storeName,
                                               final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                REMOVE_UNIQ_INDEX,
                REMOVE_UNIQ_INDEX_RATE_DESCRIPTION,
                REMOVE_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION,
                REMOVE_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }


    public static Sensor restoreNonUniqIndexSensor(final String taskId,
                                                final String storeType,
                                                final String storeName,
                                                final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                REBUILD_NON_UNIQ_INDEX,
                REBUILD_NON_UNIQ_INDEX_RATE_DESCRIPTION,
                REBUILD_NON_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION,
                REBUILD_NON_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    public static Sensor lookupNonUniqIndexSensor(final String taskId,
                                               final String storeType,
                                               final String storeName,
                                               final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                LOOKUP_NON_UNIQ_INDEX,
                LOOKUP_NON_UNIQ_INDEX_RATE_DESCRIPTION,
                LOOKUP_NON_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION,
                LOOKUP_NON_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    public static Sensor updateNonUniqIndexSensor(final String taskId,
                                               final String storeType,
                                               final String storeName,
                                               final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                UPDATE_NON_UNIQ_INDEX,
                UPDATE_NON_UNIQ_INDEX_RATE_DESCRIPTION,
                UPDATE_NON_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION,
                UPDATE_NON_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    public static Sensor removeNonUniqIndexSensor(final String taskId,
                                               final String storeType,
                                               final String storeName,
                                               final StreamsMetricsImpl streamsMetrics) {
        return throughputAndLatencySensor(
                taskId, storeType,
                storeName,
                REMOVE_NON_UNIQ_INDEX,
                REMOVE_NON_UNIQ_INDEX_RATE_DESCRIPTION,
                REMOVE_NON_UNIQ_INDEX_AVG_LATENCY_DESCRIPTION,
                REMOVE_NON_UNIQ_INDEX_MAX_LATENCY_DESCRIPTION,
                Sensor.RecordingLevel.DEBUG,
                streamsMetrics
        );
    }

    private static Sensor throughputAndLatencySensor(final String taskId,
                                                     final String storeType,
                                                     final String storeName,
                                                     final String metricName,
                                                     final String descriptionOfRate,
                                                     final String descriptionOfAvg,
                                                     final String descriptionOfMax,
                                                     final Sensor.RecordingLevel recordingLevel,
                                                     final StreamsMetricsImpl streamsMetrics) {
        final Sensor sensor;
        final String latencyMetricName = metricName + LATENCY_SUFFIX;
        final Map<String, String> tagMap = streamsMetrics.storeLevelTagMap(taskId, storeType, storeName);
        sensor = streamsMetrics.storeLevelSensor(taskId, storeName, metricName, recordingLevel);
        addInvocationRateToSensor(sensor, STATE_STORE_LEVEL_GROUP, tagMap, metricName, descriptionOfRate);
        addAvgAndMaxToSensor(
                sensor,
                STATE_STORE_LEVEL_GROUP,
                tagMap,
                latencyMetricName,
                descriptionOfAvg,
                descriptionOfMax
        );
        return sensor;
    }
}
