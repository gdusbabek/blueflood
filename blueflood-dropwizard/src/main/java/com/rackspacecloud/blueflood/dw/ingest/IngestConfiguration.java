package com.rackspacecloud.blueflood.dw.ingest;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.rackspacecloud.blueflood.dw.CommonConfiguration;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class IngestConfiguration extends CommonConfiguration {

    private String host = "127.0.0.1";
    private int port = 19000;
    
    // debug setting where the API endpoint ignore collection times passed in.
    private boolean forceNewCollectionTime = false;
    

    @JsonProperty
    public String getHost() { return host; }

    @JsonProperty
    public void setHost(String host) { this.host = host; } 

    @JsonProperty
    public int getPort() { return port; }

    @JsonProperty
    public void setPort(int port) { this.port = port; }

    @JsonProperty
    public boolean getForceNewCollectionTime() { return forceNewCollectionTime; }

    @JsonProperty
    public void setforceNewCollectionTime(boolean forceNewCollectionTime) { this.forceNewCollectionTime = forceNewCollectionTime; }

}

