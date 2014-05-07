package com.rackspacecloud.blueflood.dw.fake;

import com.rackspacecloud.blueflood.io.ShardStateIO;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.SlotState;
import com.rackspacecloud.blueflood.service.UpdateStamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FakeShardStateIO implements ShardStateIO {
    
    private Map<Integer, Map<Granularity, Map<Integer, UpdateStamp>>> map = new HashMap<Integer, Map<Granularity, Map<Integer, UpdateStamp>>>();
            
    @Override
    public synchronized Collection<SlotState> getShardState(int shard) throws IOException {
        Map<Granularity, Map<Integer, UpdateStamp>> updates = map.get(shard);
        if (updates == null) {
            return new ArrayList<SlotState>();
        } else {
            List<SlotState> states = new ArrayList<SlotState>();
            for (Map.Entry<Granularity, Map<Integer, UpdateStamp>> e0 : updates.entrySet()) {
                for (Map.Entry<Integer, UpdateStamp> e1 : e0.getValue().entrySet()) {
                    states.add(new SlotState(e0.getKey(), e1.getKey(), e1.getValue().getState()));
                }
            }
            return states;
        }
    }

    @Override
    public synchronized void putShardState(int shard, Map<Granularity, Map<Integer, UpdateStamp>> slotTimes) throws IOException {
        map.put(shard, slotTimes);
    }
}
