package com.rackspacecloud.blueflood.dw.fake;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.rackspacecloud.blueflood.io.MetadataIO;
import com.rackspacecloud.blueflood.types.Locator;

import java.io.IOException;
import java.util.Map;

public class FakeMetadataIO implements MetadataIO {
    
    private final Table<Locator, String, String> backingTable = Tables.newCustomTable(
        Maps.<Locator, Map<String, String>>newHashMap(),
        new Supplier<Map<String, String>>() {
            @Override
            public Map<String, String> get() {
                return Maps.newHashMap();
            }
        }
    );
    
    @Override
    public synchronized void put(Locator locator, String key, String value) throws IOException {
        backingTable.put(locator, key, value);
    }

    @Override
    public synchronized Map<String, String> getAllValues(Locator locator) throws IOException {
        return backingTable.row(locator);
    }

    @Override
    public synchronized int getNumberOfRowsTest() throws IOException {
        return backingTable.rowKeySet().size();
    }
}
