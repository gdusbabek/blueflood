package com.rackspacecloud.blueflood.dw;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.rackspacecloud.blueflood.dw.types.Graphite;
import com.rackspacecloud.blueflood.dw.types.Riemann;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class CommonConfiguration extends Configuration {
    @NotEmpty
    private List<String> cassandraHosts = Lists.newArrayList("127.0.0.1:19180");
    
    private int cassandraRequestTimeout = 10000;
    private int cassandraMaxRetries = 5;
    private int cassandraDefaultPort = 19180;
    
    @NotEmpty
    private String rollupKeyspace = "DATA";
    
    private int shardPushPeriod = 2000;
    private int shardPullPeriod = 20000;
    
    private Riemann riemann;
    private Graphite graphite;
    
    private boolean populateBluefloodConfigurationSettings = false;
    
    @NotEmpty 
    private String metricsWriterClass = "com.rackspacecloud.blueflood.io.AstyanaxMetricsWriter";
    
    private String luceneDirectory = new File("bf_index").getAbsolutePath();
    
    @JsonProperty
    public String getLuceneDirectory() { return luceneDirectory; }

    @JsonProperty
    public void setLuceneDirectory(String luceneDirectory) { this.luceneDirectory = luceneDirectory; }
    
    @JsonProperty
    public String getMetricsWriterClass() { return metricsWriterClass; }

    @JsonProperty
    public void setMetricsWriterClass(String metricsWriterClass) { this.metricsWriterClass = metricsWriterClass; }
    
    @JsonProperty
    public void setCassandraHosts(List<String> l) { this.cassandraHosts = l; }
        
    @JsonProperty
    public List<String> getCassandraHosts() { return Collections.unmodifiableList(cassandraHosts); }
    
    @JsonProperty
    public int getCassandraRequestTimeout() { return cassandraRequestTimeout; }

    @JsonProperty
    public void setCassandraRequestTimeout(int cassandraRequestTimeout) { this.cassandraRequestTimeout = cassandraRequestTimeout; }

    @JsonProperty
    public int getCassandraMaxRetries() { return cassandraMaxRetries; }

    @JsonProperty
    public void setCassandraMaxRetries(int cassandraMaxRetries) { this.cassandraMaxRetries = cassandraMaxRetries; }

    @JsonProperty
    public int getCassandraDefaultPort() { return cassandraDefaultPort; }

    @JsonProperty
    public void setCassandraDefaultPort(int cassandraDefaultPort) { this.cassandraDefaultPort = cassandraDefaultPort; }

    @JsonProperty
    public String getRollupKeyspace() { return rollupKeyspace; }

    @JsonProperty
    public void setRollupKeyspace(String rollupKeyspace) { this.rollupKeyspace = rollupKeyspace; }

    @JsonProperty
    public int getShardPushPeriod() { return shardPushPeriod; }

    @JsonProperty
    public void setShardPushPeriod(int shardPushPeriod) { this.shardPushPeriod = shardPushPeriod; }

    @JsonProperty
    public int getShardPullPeriod() { return shardPullPeriod; }

    @JsonProperty
    public void setShardPullPeriod(int shardPullPeriod) { this.shardPullPeriod = shardPullPeriod; }
    
    @JsonProperty
    public Riemann getRiemann() { return riemann; }

    @JsonProperty
    public void setRiemann(Riemann riemann) { this.riemann = riemann; }

    @JsonProperty
    public Graphite getGraphite() { return graphite; }

    @JsonProperty
    public void setGraphite(Graphite graphite) { this.graphite = graphite; }
    
    @JsonProperty
    public boolean isPopulateBluefloodConfigurationSettings() { return populateBluefloodConfigurationSettings; }

    @JsonProperty
    public void setPopulateBluefloodConfigurationSettings(boolean populateBluefloodConfigurationSettings) {
        this.populateBluefloodConfigurationSettings = populateBluefloodConfigurationSettings; 
    }
    
}

// at some point, when DW query is ready, this class will be factored into `BaseConfiguration` which will hold all
// the common configurations to both ingest and query. For now, keep everything here.


