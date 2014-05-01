package com.rackspacecloud.blueflood.dw.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.rackspacecloud.blueflood.dw.CommonConfiguration;

public class QueryConfiguration extends CommonConfiguration {
    private String luceneDirectory;

    @JsonProperty
    public String getLuceneDirectory() {
        return luceneDirectory;
    }

    @JsonProperty
    public void setLuceneDirectory(String lucendDirectory) {
        this.luceneDirectory = lucendDirectory;
    }
}
