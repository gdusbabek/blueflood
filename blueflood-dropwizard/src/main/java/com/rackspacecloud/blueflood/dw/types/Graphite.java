package com.rackspacecloud.blueflood.dw.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Graphite {
    private boolean enabled;
    private String host;
    private int port;
    private String prefix;

    @JsonProperty
    public boolean isEnabled() { return enabled; }

    @JsonProperty
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    @JsonProperty
    public String getHost() { return host; }

    @JsonProperty
    public void setHost(String host) { this.host = host; }

    @JsonProperty
    public int getPort() { return port; }

    @JsonProperty
    public void setPort(int port) { this.port = port; }

    @JsonProperty
    public String getPrefix() { return prefix; }

    @JsonProperty
    public void setPrefix(String prefix) { this.prefix = prefix; }
}
