package com.rackspacecloud.blueflood.io;

import java.util.List;

import com.rackspacecloud.blueflood.types.IMetric;

public interface DiscoveryIO {
    public void insertDiscovery(List<? extends IMetric> metrics) throws Exception;
}
