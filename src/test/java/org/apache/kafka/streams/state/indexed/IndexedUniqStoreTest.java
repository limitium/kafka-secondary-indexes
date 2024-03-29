package org.apache.kafka.streams.state.indexed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class IndexedUniqStoreTest {

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
                //Build uniq index based on first char
                .addUniqIndex("idx0", (v) -> v) //another index to check consistency on uniq key violation
                .addUniqIndex("idx", (v) -> String.valueOf(v.charAt(0)));

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
        store.put(2, "bb");
        store.put(3, "cc");

        assertEquals("aa", store.getUnique("idx", "a"));
        assertEquals("bb", store.getUnique("idx", "b"));
        assertEquals("cc", store.getUnique("idx", "c"));
    }

    @Test
    void shouldThrowUniqKeyViolationForTheSameIndexKey() {
        store.put(1, "aa");
        assertEquals(1, store.getUniqueKey("idx", "a"), "Failed to find object via index");

        assertThrows(UniqKeyViolationException.class, () -> store.put(2, "ab"));

        assertEquals(1, store.getUniqueKey("idx", "a"), "Failed update, broke uniq index");
        assertNull(store.getUniqueKey("idx0", "ab"), "Other indexes left dirty on uniq key violation");
        assertNull(store.get(2),"Value was stored on uniq key violation");
    }

    @Test
    void shouldRemoveValueFromIndexOnDelete() {
        store.put(1, "aa");
        assertEquals("aa", store.getUnique("idx", "a"));
        assertEquals(1, store.getUniqueKey("idx", "a"));

        store.delete(1);
        assertNull(store.get(1),"Value wasn't deleted");
        assertNull(store.getUnique("idx", "a"), "Index has deleted value");
        assertNull(store.getUniqueKey("idx", "a"),"Index key wasn't deleted");
    }

    @Test
    void shouldThrowRuntimeExceptionOnNonImplementedMethods() {
        assertThrows(RuntimeException.class, () -> store.putAll(null));
        assertThrows(RuntimeException.class, () -> store.putIfAbsent(null, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldRebuildIndexOnRestore() {
        store.close();

        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addEntryToRestoreLog(0, "aa");
        driver.addEntryToRestoreLog(1, "bb");
        driver.addEntryToRestoreLog(2, "cc");
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

        assertEquals("aa", store.getUnique("idx", "a"));
        assertEquals("bb", store.getUnique("idx", "b"));
        assertNull(store.getUnique("idx", "c"));
    }
}
