package com.rackspacecloud.blueflood.dw.ingest;

import com.rackspacecloud.blueflood.dw.fake.FakeShardStateIO;
import com.rackspacecloud.blueflood.io.ShardStateIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.ScheduleContext;
import com.rackspacecloud.blueflood.service.ShardStatePuller;
import com.rackspacecloud.blueflood.service.ShardStatePusher;
import com.rackspacecloud.blueflood.utils.Util;
import io.dropwizard.lifecycle.Managed;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

public class TestStateManager {
    
    @Test
    public void testStartStop() throws Exception {
        assertIngestMode();
        ShardStateIO io = new FakeShardStateIO();
        ScheduleContext rollupContext = new ScheduleContext(System.currentTimeMillis(), Util.parseShards("NONE"));
        Managed stateManager = new StateManager(rollupContext, io);
        
        Object shardStateServices = Whitebox.getInternalState(stateManager, "services");
        ShardStatePusher pusher = (ShardStatePusher)Whitebox.getInternalState(shardStateServices, "pusher");
        ShardStatePuller puller = (ShardStatePuller)Whitebox.getInternalState(shardStateServices, "puller");
        
        // verify they are not active.
        Assert.assertFalse(pusher.getActive());
        Assert.assertFalse(puller.getActive());
        
        stateManager.start();
        
        // verify they are active.
        Assert.assertTrue(pusher.getActive());
        Assert.assertTrue(puller.getActive());
        
        stateManager.stop();
        
        // verify they are not active.
        Assert.assertFalse(pusher.getActive());
        Assert.assertFalse(puller.getActive());
        
    }
    
    private static void assertIngestMode() {
        // config.getBooleanProperty(CoreConfig.INGEST_MODE
        System.setProperty(CoreConfig.INGEST_MODE.name(), "true");
        Assert.assertTrue(Configuration.getInstance().getBooleanProperty(CoreConfig.INGEST_MODE));
    }
}
