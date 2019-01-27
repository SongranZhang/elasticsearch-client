package com.linkedkeeper.elasticsearch.client;

import java.io.Serializable;

public class IndexField implements Serializable {

    private String name;
    private String index;
    private String type;

    public IndexField(String name, String type, String index) {
        this.name = name;
        this.type = type;
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }
}
