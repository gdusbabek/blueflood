/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.inputs.processors;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class DiscoveryWriter extends AsyncFunctionWithThreadPool<List<List<Metric>>, List<List<Metric>>> {

    private final List<DiscoveryIO> discoveryIOs = new ArrayList<DiscoveryIO>();
    private final Map<Class<? extends DiscoveryIO>, Timer> writeDurationTimers = new HashMap<Class<? extends DiscoveryIO>, Timer>();
    private final Map<Class<? extends DiscoveryIO>, Meter> writeErrorMeters = new HashMap<Class<? extends DiscoveryIO>, Meter>();
    private static final Logger log = LoggerFactory.getLogger(DiscoveryWriter.class);

    public DiscoveryWriter(ThreadPoolExecutor threadPool) {
        super(threadPool);
        registerIOModules();
    }

    public void registerIO(DiscoveryIO io) {
        discoveryIOs.add(io);
        writeDurationTimers.put(io.getClass(),
                Metrics.timer(io.getClass(), "DiscoveryWriter Write Duration")
                );
        writeErrorMeters.put(io.getClass(),
                Metrics.meter(io.getClass(), "DiscoveryWriter Write Errors")
                );
    }

    public void registerIOModules() {
        List<String> modules = Configuration.getInstance().getListProperty(CoreConfig.DISCOVERY_MODULES);

        ClassLoader classLoader = DiscoveryIO.class.getClassLoader();
        for (String module : modules) {
            log.info("Loading metric discovery module " + module);
            try {
                Class discoveryClass = classLoader.loadClass(module);
                DiscoveryIO discoveryIOModule = (DiscoveryIO) discoveryClass.newInstance();
                log.info("Registering metric discovery module " + module);
                registerIO(discoveryIOModule);
            } catch (InstantiationException e) {
                log.error("Unable to create instance of metric discovery class for: " + module, e);
            } catch (IllegalAccessException e) {
                log.error("Error starting metric discovery module: " + module, e);
            } catch (ClassNotFoundException e) {
                log.error("Unable to locate metric discovery module: " + module, e);
            } catch (RuntimeException e) {
                log.error("Error starting metric discovery module: " + module, e);
            } catch (Throwable e) {
                log.error("Error starting metric discovery module: " + module, e);
            }
        }
    }

    public ListenableFuture<List<Boolean>> processMetrics(List<List<Metric>> input) {
        final List<ListenableFuture<Boolean>> resultFutures = new ArrayList<ListenableFuture<Boolean>>();
        for (final List<Metric> metrics : input) {
            ListenableFuture<Boolean> futureBatchResult = getThreadPool().submit(new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    boolean success = true;
                        for (DiscoveryIO io : discoveryIOs) {
                            Timer.Context actualWriteCtx = writeDurationTimers.get(io.getClass()).time();
                            try {
                                io.insertDiscovery(metrics);
                            } catch (Exception ex) {
                                getLogger().error(ex.getMessage(), ex);
                                writeErrorMeters.get(io.getClass()).mark();
                                success = false;
                            } finally {
                                actualWriteCtx.stop();
                            }
                        }
                        return success;
                }
            });
            resultFutures.add(futureBatchResult);
        }
        return Futures.allAsList(resultFutures);
    }

    public ListenableFuture<List<List<Metric>>> apply(List<List<Metric>> input) {
        processMetrics(input);
        // we don't need all metrics to finish being inserted into the discovery backend
        // before moving onto the next step in the processing chain.
        return new NoOpFuture<List<List<Metric>>>(input);
    }
}
