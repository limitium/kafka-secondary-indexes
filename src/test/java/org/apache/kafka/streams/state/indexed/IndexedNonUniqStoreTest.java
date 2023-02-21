package org.apache.kafka.streams.state.indexed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.IndexedKeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.Stores2;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.*;

public class IndexedNonUniqStoreTest {

    protected InternalMockProcessorContext<Integer, String> context;
    protected IndexedKeyValueStore<Integer, String> store;
    protected KeyValueStoreTestDriver<Integer, String> driver;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() {
        driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        context = (InternalMockProcessorContext<Integer, String>) driver.context();
        context.setTime(10);

        store = createStore(context);
        store.rebuildIndexes();
    }

    private IndexedKeyValueStore<Integer, String> createStore(InternalMockProcessorContext<Integer, String> context) {
        IndexedKeyValueStoreBuilder<Integer, String> builder = Stores2.keyValueStoreBuilder(
                        Stores.lruMap("my-store", 10),
                        Serdes.Integer(),
                        Serdes.String())
                //Build non uniq index based on first char
                .addNonUniqIndex("idx", (v) -> String.valueOf(v.charAt(0)));

        IndexedKeyValueStore<Integer, String> store = builder.build();

        store.init((StateStoreContext) context, store);
        return store;
    }

    @AfterEach
    public void clean() {
        store.close();
        driver.clear();
    }


    @Test
    void shouldReturnIndexedValue() {
        store.put(1, "aa");
        store.put(2, "ab");
        store.put(3, "ac");
        store.put(4, "ba");
        store.put(5, "cc");

        assertThat(store.getNonUnique("idx", "a").collect(Collectors.toSet()), hasItems("aa", "ab", "ac"));

        assertEquals(1, store.getNonUnique("idx", "b").count());
    }

    @Test
    void shouldRemoveValueFromIndexOnDelete() {
        store.put(1, "aa");
        assertThat(store.getNonUnique("idx", "a").collect(Collectors.toSet()), hasItems("aa"));

        store.delete(1);
        assertEquals(0, store.getNonUnique("idx", "a").count());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldRebuildIndexOnRestore() {
        store.close();

        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "aa");
        driver.addEntryToRestoreLog(1, "ab");
        driver.addEntryToRestoreLog(2, "ac");
        driver.addEntryToRestoreLog(2, null);

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        store = createStore((InternalMockProcessorContext<Integer, String>) driver.context());
        context.restore(store.name(), driver.restoredEntries());
        store.rebuildIndexes();

        // Verify that the store's changelog does not get more appends ...
        assertEquals(0, driver.numFlushedEntryStored());
        assertEquals(0, driver.numFlushedEntryRemoved());

        // and there are no other entries ...
        assertEquals(2, driver.sizeOf(store));

        assertThat(store.getNonUnique("idx", "a").collect(Collectors.toSet()), hasItems("aa", "ab"));
    }
}
