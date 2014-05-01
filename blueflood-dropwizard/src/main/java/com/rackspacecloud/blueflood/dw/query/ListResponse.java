package com.rackspacecloud.blueflood.dw.query;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ListResponse {
    private List<String> data;
    
    public ListResponse(Collection<String> data) {
        this.data = new ArrayList<String>(data);
    }

    @JsonProperty
    public List<String> getData() {
        return data;
    }

    @JsonProperty
    public void setData(List<String> data) {
        this.data = data;
    }
}
