package org.apache.kafka.streams.state.indexed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.Stores2;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class IndexesNonUniqStoreExceptionsTest {
    protected InternalMockProcessorContext<Integer, String> context;
    protected IndexedKeyValueStore<Integer, String> store;
    protected KeyValueStoreTestDriver<Integer, String> driver;

    @BeforeEach
    public void setUp() {
        driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        context = (InternalMockProcessorContext<Integer, String>) driver.context();
        context.setTime(10);
        IndexedKeyValueStoreBuilder<Integer, String> builder = Stores2.keyValueStoreBuilder(
                        Stores.lruMap("my-store", 10),
                        Serdes.Integer(),
                        Serdes.String())
                //Return null key index
                .addNonUniqIndex("nidx", (v) -> null);

        store = builder.build();

        store.init((StateStoreContext) context, store);
    }


    @Test
    void shouldThrowNPEOnNullIndexKey() {
        assertThrows(NullPointerException.class, () -> store.put(0, "aaa"));
    }

    @Test
    void shouldThrowRuntimeExceptionOnMissedRebuildIndexesCall() {
        assertThrows(RuntimeException.class, () -> store.getNonUnique("nidx", "a"));
    }
}
