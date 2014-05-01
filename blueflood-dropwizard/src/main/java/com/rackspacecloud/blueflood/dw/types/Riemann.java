package com.rackspacecloud.blueflood.dw.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Riemann {
    private boolean enabled = false;
    private String host;
    private int port;
    private String prefix;
    private String localhost;
    private String[] tags;
    private String separator;
    private int ttl;

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

    @JsonProperty
    public String getLocalhost() { return localhost; }

    @JsonProperty
    public void setLocalhost(String localhost) { this.localhost = localhost; }

    @JsonProperty
    public String[] getTags() { return tags; }

    @JsonProperty
    public void setTags(String[] tags) { this.tags = tags; }

    @JsonProperty
    public String getSeparator() { return separator; }

    @JsonProperty
    public void setSeparator(String separator) { this.separator = separator; }

    @JsonProperty
    public int getTtl() { return ttl; }

    @JsonProperty
    public void setTtl(int ttl) { this.ttl = ttl; }
    
    
}