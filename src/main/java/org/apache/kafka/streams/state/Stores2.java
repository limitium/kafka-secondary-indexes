package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.IndexedMeteredKeyValueStore;

import java.util.Objects;

public class Stores2 {
    /**
     * Creates a {@link IndexedKeyValueStoreBuilder} that can be used to build a {@link IndexedMeteredKeyValueStore}.
     * <p>
     * The provided supplier should <strong>not</strong> be a supplier for
     * {@link TimestampedKeyValueStore TimestampedKeyValueStores}.
     *
     * To access to store from {@link org.apache.kafka.streams.processor.api.Processor#init(ProcessorContext)}
     * <pre> {@code
     *         @Override
     *         public void init(ProcessorContext<K, V> context) {
     *             Processor.super.init(context);
     *
     *             IndexedKeyValueStore<K, V> indexedStore = ((WrappedStateStore<IndexedKeyValueStore<K, V>, K, V>)context.getStateStore("store-name")).wrapped();
     *
     *             indexedStore.rebuildIndexes();
     *         }
     * }</pre>
     *
     *
     * @param supplier   a {@link KeyValueBytesStoreSupplier} (cannot be {@code null})
     * @param keySerde   the key serde to use
     * @param valueSerde the value serde to use; if the serialized bytes is {@code null} for put operations,
     *                   it is treated as delete
     * @param <K>        key type
     * @param <V>        value type
     * @return an instance of a {@link IndexedKeyValueStoreBuilder} that can build a {@link IndexedMeteredKeyValueStore}
     */
    public static <K, V> IndexedKeyValueStoreBuilder<K, V> keyValueStoreBuilder(final KeyValueBytesStoreSupplier supplier,
                                                                                final Serde<K> keySerde,
                                                                                final Serde<V> valueSerde) {
        Objects.requireNonNull(supplier, "supplier cannot be null");
        return new IndexedKeyValueStoreBuilder<>(supplier, keySerde, valueSerde, Time.SYSTEM);
    }
}
