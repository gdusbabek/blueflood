package com.rackspacecloud.blueflood.dw.ingest;

import com.rackspacecloud.blueflood.inputs.processors.DiscoveryWriter;
import com.rackspacecloud.blueflood.types.IMetric;
import io.dropwizard.lifecycle.Managed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

public class DiscoveryManager implements Managed {

    private final ThreadPoolExecutor threadPool;
    private final DiscoveryWriter writer;

    private DiscoveryManager() {
        threadPool = null;
        writer = null;
    }
    
    public DiscoveryManager(ThreadPoolExecutor threadPool) {
        this.threadPool = threadPool;
        writer = new DiscoveryWriter(threadPool);
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
        threadPool.shutdown();
    }
    
    public void insertDiscoveryAsync(final Collection<? extends IMetric> metrics) {
        // yay for crappy interfaces, mea culpa and all that...
        List<List<? extends IMetric>> listOfLists = new ArrayList<List<? extends IMetric>>();
        List<? extends IMetric> list = new ArrayList<IMetric>(metrics);
        listOfLists.add(list);
        writer.processMetrics(listOfLists);
    }
}
