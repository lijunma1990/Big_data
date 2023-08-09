package com.drlee.pojo;

import java.io.Serializable;

/**
 * \* Created by Drlee on 2023-03-16.
 */
public class WaterSensor implements Serializable {
    private String id;
    private long ts;
    private int vc;

    public WaterSensor() {

    }

    public WaterSensor(String id, long ts, int vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public int getVc() {
        return vc;
    }

    public void setVc(int vc) {
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}


