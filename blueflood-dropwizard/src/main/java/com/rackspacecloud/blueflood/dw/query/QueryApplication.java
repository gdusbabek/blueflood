package com.rackspacecloud.blueflood.dw.query;

import com.rackspacecloud.blueflood.dw.NotDOAHealthCheck;
import com.rackspacecloud.blueflood.dw.StateManager;
import com.rackspacecloud.blueflood.dw.ingest.IngestApplication;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.utils.Util;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class QueryApplication extends Application<QueryConfiguration> {
    private static final String NAME = "blueflood-query";
    
    public static void main(String args[]) throws Exception {
        new QueryApplication().run(args);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void initialize(Bootstrap<QueryConfiguration> bootstrap) {}

    @Override
    public void run(QueryConfiguration configuration, Environment environment) throws Exception {
        final ScheduleContext rollupContext = new ScheduleContext(System.currentTimeMillis(), Util.parseShards("NONE"));
        
        // should we inject the Dropwizard config into Blueflood? 
        if (configuration.isPopulateBluefloodConfigurationSettings()) {
            IngestApplication.overrideBluefloodConfiguration(configuration);
        }
        
        // shard state management.
        StateManager stateManager = new StateManager(rollupContext);
        environment.lifecycle().manage(stateManager);
        
        // should not need a metadata cache.
        
        // allows us to limit returned fields based on the "select" parameter.
        environment.getObjectMapper().setFilters(new MetricFilterProvider());
        
        final NotDOAHealthCheck notDOA = new NotDOAHealthCheck();
        final QueryResource queryResource = new QueryResource();
        final LuceneResource luceneResource = new LuceneResource();
        
        // register
        environment.healthChecks().register("not-doa", notDOA);
        environment.jersey().register(queryResource);
        environment.jersey().register(luceneResource);
        
    }
}