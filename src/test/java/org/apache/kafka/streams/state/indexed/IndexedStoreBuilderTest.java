package org.apache.kafka.streams.state.indexed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.easymock.EasyMock.createMock;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IndexedStoreBuilderTest {

    @Test
    void shouldThrowNullPointerIfIndexNameIsNull() {
        assertThrows(NullPointerException.class, () -> Stores2.keyValueStoreBuilder(createMock(KeyValueBytesStoreSupplier.class), Serdes.String(), Serdes.String()).addUniqIndex(null, (v) -> null));
        assertThrows(NullPointerException.class, () -> Stores2.keyValueStoreBuilder(createMock(KeyValueBytesStoreSupplier.class), Serdes.String(), Serdes.String()).addNonUniqIndex(null, (v) -> null));
    }

    @Test
    void shouldThrowNullPointerIfKeyGeneratorIsNull() {
        assertThrows(NullPointerException.class, () -> Stores2.keyValueStoreBuilder(createMock(KeyValueBytesStoreSupplier.class), Serdes.String(), Serdes.String()).addUniqIndex("index", null));
        assertThrows(NullPointerException.class, () -> Stores2.keyValueStoreBuilder(createMock(KeyValueBytesStoreSupplier.class), Serdes.String(), Serdes.String()).addNonUniqIndex("nindex", null));
    }

    @Test
    void shouldThrowRuntimeExceptionIfSameIndexNameIsUsed() {
        assertThrows(RuntimeException.class, () -> Stores2.keyValueStoreBuilder(createMock(KeyValueBytesStoreSupplier.class), Serdes.String(), Serdes.String()).addUniqIndex("index", (v) -> v).addUniqIndex("index", (v) -> v));
        assertThrows(RuntimeException.class, () -> Stores2.keyValueStoreBuilder(createMock(KeyValueBytesStoreSupplier.class), Serdes.String(), Serdes.String()).addNonUniqIndex("nindex", (v) -> v).addNonUniqIndex("nindex", (v) -> v));
    }

}
