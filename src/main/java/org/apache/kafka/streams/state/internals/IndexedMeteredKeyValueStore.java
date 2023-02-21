package org.apache.kafka.streams.state.internals;


import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.UniqKeyViolationException;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;


public class IndexedMeteredKeyValueStore<K, V> extends MeteredKeyValueStore<K, V> implements IndexedKeyValueStore<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(IndexedMeteredKeyValueStore.class);
    private final String metricsScope;
    private Sensor rebuildUniqIndexSensor;
    private Sensor lookupUniqIndexSensor;
    private Sensor updateUniqIndexSensor;
    private Sensor removeUniqIndexSensor;
    private Sensor rebuildNonUniqIndexSensor;
    private Sensor lookupNonUniqIndexSensor;
    private Sensor updateNonUniqIndexSensor;
    private Sensor removeNonUniqIndexSensor;

    private final Map<String, Map<String, K>> uniqIndexesData = new HashMap<>();
    private final Map<String, Map<String, Set<K>>> nonUniqIndexesData = new HashMap<>();
    private final Map<String, Function<V, String>> uniqIndexes;
    private final Map<String, Function<V, String>> nonUniqIndexes;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean indexesBuilt = false;

    IndexedMeteredKeyValueStore(final Map<String, Function<V, String>> uniqIndexes,
                                final Map<String, Function<V, String>> nonUniqIndexes,
                                final KeyValueStore<Bytes, byte[]> inner,
                                final String metricsScope,
                                final Time time,
                                final Serde<K> keySerde,
                                final Serde<V> valueSerde) {
        super(inner, metricsScope, time, keySerde, valueSerde);
        logger.debug("Store `{}` created with {} uniq, {} non uniq indexes", name(), uniqIndexes.size(), nonUniqIndexes.size());

        this.metricsScope = metricsScope;
        this.uniqIndexes = uniqIndexes;
        this.nonUniqIndexes = nonUniqIndexes;


        this.uniqIndexes.forEach((name, generator) -> uniqIndexesData.put(name, new HashMap<>()));
        this.nonUniqIndexes.forEach((name, generator) -> nonUniqIndexesData.put(name, new HashMap<>()));
    }


    @Override
    public void init(StateStoreContext context, StateStore root) {
        super.init(context, root);
        TaskId taskId = context.taskId();
        StreamsMetricsImpl streamsMetrics = (StreamsMetricsImpl) context.metrics();

        rebuildUniqIndexSensor = StateStoreMetrics2.restoreUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        lookupUniqIndexSensor = StateStoreMetrics2.lookupUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        updateUniqIndexSensor = StateStoreMetrics2.updateUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        removeUniqIndexSensor = StateStoreMetrics2.removeUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);

        rebuildNonUniqIndexSensor = StateStoreMetrics2.restoreNonUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        lookupNonUniqIndexSensor = StateStoreMetrics2.lookupNonUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        updateNonUniqIndexSensor = StateStoreMetrics2.updateNonUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        removeNonUniqIndexSensor = StateStoreMetrics2.removeNonUniqIndexSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
    }

    @Override
    public void rebuildIndexes() {
        lock.writeLock().lock();

        try {
            if (indexesBuilt) { //No reason to rebuild in each Processor
                return;
            }
            uniqIndexesData.values().forEach(Map::clear);
            nonUniqIndexesData.values().forEach(Map::clear);

            try (KeyValueIterator<Bytes, byte[]> kvIterator = wrapped().all()) {
                while (kvIterator.hasNext()) {
                    KeyValue<Bytes, byte[]> kv = kvIterator.next();

                    K key = deserKey(kv.key);
                    V value = deserValue(kv.value);

                    maybeMeasureLatency(() -> updateUniqIndexes(key, value), time, rebuildUniqIndexSensor);
                    maybeMeasureLatency(() -> updateNonUniqIndexes(key, value), time, rebuildNonUniqIndexSensor);
                }
            }
            indexesBuilt = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V getUnique(String indexName, String indexKey) {
        lock.readLock().lock();
        try {
            K key = getUniqueKey(indexName, indexKey);
            if(key != null) {
                return get(key);
            }
            return null;
        }finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public K getUniqueKey(String indexName, String indexKey) {
        Objects.requireNonNull(indexName, "indexName cannot be null");
        Objects.requireNonNull(indexKey, "indexKey cannot be null");

        lock.readLock().lock();
        if (!indexesBuilt) {
            throw new RuntimeException("Indexes were not built, call IndexedKeyValueStore.rebuildIndexes() from Processor#init() method");
        }
        try {
            return maybeMeasureLatency(() -> lookupUniqKey(indexName, indexKey), time, lookupUniqIndexSensor);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Stream<V> getNonUnique(String indexName, String indexKey) {
        Objects.requireNonNull(indexName, "indexName cannot be null");
        Objects.requireNonNull(indexKey, "indexKey cannot be null");

        lock.readLock().lock();
        if (!indexesBuilt) {
            throw new RuntimeException("Indexes were not built, call IndexedKeyValueStore.rebuildIndexes() from Processor#init() method");
        }
        try {
            Stream<K> keys = maybeMeasureLatency(() -> lookupNonUniqKeys(indexName, indexKey), time, lookupNonUniqIndexSensor);

            return keys.map(super::get);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        lock.writeLock().lock();
        try {
            maybeMeasureLatency(() -> updateUniqIndexes(key, value), time, updateUniqIndexSensor);
            maybeMeasureLatency(() -> updateNonUniqIndexes(key, value), time, updateNonUniqIndexSensor);
            super.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public V delete(K key) {
        lock.writeLock().lock();
        try {
            V deleted = super.delete(key);
            if (deleted != null) {
                maybeMeasureLatency(() -> removeUniqIndex(key, deleted), time, removeUniqIndexSensor);
                maybeMeasureLatency(() -> removeNonUniqIndex(key, deleted), time, removeNonUniqIndexSensor);
            }
            return deleted;
        } finally {
            lock.writeLock().unlock();
        }
    }


    private K lookupUniqKey(String indexName, String indexKey) {
        Map<String, K> index = uniqIndexesData.get(indexName);
        Objects.requireNonNull(index, "Index not found:" + indexName);

        return index.get(indexKey);
    }

    private Stream<K> lookupNonUniqKeys(String indexName, String indexKey) {
        Map<String, Set<K>> index = nonUniqIndexesData.get(indexName);
        Objects.requireNonNull(index, "Index not found:" + indexName);

        Set<K> keys = new HashSet<>(Optional.ofNullable(index.get(indexKey))
                .orElse(Collections.emptySet()));

        return keys.stream();
    }

    private void removeUniqIndex(K key, V value) {
        uniqIndexesData.forEach((indexName, indexData) -> {
            String indexKey = generateIndexKey(uniqIndexes, indexName, value);

            logger.debug("Remove from uniq index `{}` key `{}`, for {}:{}", indexName, indexKey, key, value);
            indexData.remove(indexKey);
        });
    }

    private void removeNonUniqIndex(K key, V value) {
        nonUniqIndexesData.forEach((indexName, indexData) -> {
            String indexKey = generateIndexKey(nonUniqIndexes, indexName, value);

            logger.debug("Remove from non uniq index `{}` key `{}`, for {}:{}", indexName, indexKey, key, value);
            Set<K> keys = indexData.get(indexKey);
            if (keys != null) {
                keys.remove(key);
                if (keys.isEmpty()) {
                    indexData.remove(indexKey);
                }
            }
        });
    }

    private void insertNonUniqKey(Map<String, Set<K>> indexData, String indexKey, Bytes key) {
        if (!indexData.containsKey(indexKey)) {
            indexData.put(indexKey, new HashSet<>());
        }
        indexData.get(indexKey).add(keySerde.deserializer().deserialize(null, key.get()));
    }

    private void updateUniqIndexes(K key, V value) {

        //2N complexity vs dirty indexes and revert changes
        uniqIndexesData.forEach((indexName, indexData) -> {
            String indexKey = generateIndexKey(uniqIndexes, indexName, value);

            logger.debug("Update uniq index `{}` with key `{}`, for {}:{}", indexName, indexKey, key, value);

            K prevStoredKey = indexData.get(indexKey);
            if (prevStoredKey != null && !key.equals(prevStoredKey)) {
                throw new UniqKeyViolationException("Uniqueness violation of `" + indexName + "` index key:" + indexKey + ", for new key:" + key + ", old key:" + prevStoredKey + ", value:" + value);
            }
        });

        uniqIndexesData.forEach((indexName, indexData) -> {
            String indexKey = generateIndexKey(uniqIndexes, indexName, value);
            indexData.put(indexKey, key);
        });
    }

    private void updateNonUniqIndexes(K key, V value) {
        nonUniqIndexesData.forEach((indexName, indexData) -> {
            String indexKey = generateIndexKey(nonUniqIndexes, indexName, value);

            logger.debug("Update non uniq index `{}` with key `{}`, for {}:{}", indexName, indexKey, key, value);
            insertNonUniqKey(indexData, indexKey, keyBytes(key));
        });
    }

    private String generateIndexKey(Map<String, Function<V, String>> indexes, String indexName, V value) {
        Function<V, String> keyGenerator = indexes.get(indexName);
        String indexKey = keyGenerator.apply(value);
        Objects.requireNonNull(indexKey, "Null keys are not supported. Problem with an index:" + indexName);
        return indexKey;
    }


    private K deserKey(Bytes key) {
        return keySerde.deserializer().deserialize(null, key.get());
    }

    private V deserValue(byte[] value) {
        return valueSerde.deserializer().deserialize(null, value);
    }
}
