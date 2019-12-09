package io.piveau.hub.search.util.geo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BoundingBox {

    private Float minLon;
    private Float maxLon;
    private Float maxLat;
    private Float minLat;

    public BoundingBox() {
        this.minLon = null;
        this.maxLon = null;
        this.maxLat = null;
        this.minLat = null;
    }

    public BoundingBox(Float minLon, Float maxLon, Float maxLat, Float minLat) {
        this.minLon = minLon;
        this.maxLon = maxLon;
        this.maxLat = maxLat;
        this.minLat = minLat;
    }

    public Float getMinLon() {
        return minLon;
    }

    public void setMinLon(Float minLon) {
        this.minLon = minLon;
    }

    public Float getMaxLon() {
        return maxLon;
    }

    public void setMaxLon(Float maxLon) {
        this.maxLon = maxLon;
    }

    public Float getMaxLat() {
        return maxLat;
    }

    public void setMaxLat(Float maxLat) {
        this.maxLat = maxLat;
    }

    public Float getMinLat() {
        return minLat;
    }

    public void setMinLat(Float minLat) {
        this.minLat = minLat;
    }

    @Override
    public String toString() {
        return "BoundingBox{" +
                "minLon=" + minLon +
                ", maxLon=" + maxLon +
                ", maxLat=" + maxLat +
                ", minLat=" + minLat +
                '}';
    }
}