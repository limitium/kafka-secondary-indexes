package org.apache.kafka.streams.state.indexed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.IndexedKeyValueStoreBuilder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class MemoryConsumptionTest {
    Logger logger = LoggerFactory.getLogger(MemoryConsumptionTest.class);

    @Test
    @SuppressWarnings("unchecked")
    void inserts5kk() {
        KeyValueStoreTestDriver<Long, String> driver = KeyValueStoreTestDriver.create(Long.class, String.class);
        InternalMockProcessorContext<Long, String> context = (InternalMockProcessorContext<Long, String>) driver.context();
        context.setTime(10);
        IndexedKeyValueStoreBuilder<Long, String> builder = Stores2.keyValueStoreBuilder(new KeyValueBytesStoreSupplier() {
                    @Override
                    public String name() {
                        return "root-name";
                    }

                    @Override
                    public KeyValueStore<Bytes, byte[]> get() {
                        return new KeyValueStore<>() {
                            @Override
                            public void put(Bytes key, byte[] value) {

                            }

                            @Override
                            public byte[] putIfAbsent(Bytes key, byte[] value) {
                                return new byte[0];
                            }

                            @Override
                            public void putAll(List<KeyValue<Bytes, byte[]>> entries) {

                            }

                            @Override
                            public byte[] delete(Bytes key) {
                                return new byte[0];
                            }

                            @Override
                            public String name() {
                                return "in-store";
                            }

                            @Override
                            public void init(ProcessorContext context, StateStore root) {

                            }

                            @Override
                            public void flush() {

                            }

                            @Override
                            public void close() {

                            }

                            @Override
                            public boolean persistent() {
                                return false;
                            }

                            @Override
                            public boolean isOpen() {
                                return false;
                            }

                            @Override
                            public byte[] get(Bytes key) {
                                return new byte[0];
                            }

                            @Override
                            public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
                                return null;
                            }

                            @Override
                            public KeyValueIterator<Bytes, byte[]> all() {
                                return new KeyValueIterator<>() {
                                    @Override
                                    public void close() {

                                    }

                                    @Override
                                    public Bytes peekNextKey() {
                                        return null;
                                    }

                                    @Override
                                    public boolean hasNext() {
                                        return false;
                                    }

                                    @Override
                                    public KeyValue<Bytes, byte[]> next() {
                                        return null;
                                    }
                                };
                            }

                            @Override
                            public long approximateNumEntries() {
                                return 0;
                            }
                        };
                    }

                    @Override
                    public String metricsScope() {
                        return "metrics";
                    }
                }, Serdes.Long(), Serdes.String()).withCachingDisabled().withLoggingDisabled()
                .addUniqIndex("un", (v) -> v)
                .addNonUniqIndex("non", (v) -> String.valueOf(Long.parseLong(v) + Long.parseLong(v) % 2));

        IndexedKeyValueStore<Long, String> store = builder.build();

        store.init((StateStoreContext) context, store);


        long startSeq = System.nanoTime();
        long start = System.currentTimeMillis();
        for (long i = 0; i < 5_000_000; i++) {
            long v = startSeq++;
            store.put(v, String.valueOf(v));
            if (i % 100_000 == 0) {
                logger.info("Inserted: {}", i);
            }
        }
        float insertTime = (System.currentTimeMillis() - start) / 1000f;
        logger.info("done for: {}s", insertTime);

        System.gc();
        long memUsage = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024;
        logger.info("Mem: {}Mb", memUsage);

        assertTrue(insertTime < 4);
        assertTrue(memUsage < 1500);
    }
}

