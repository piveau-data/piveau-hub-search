package io.piveau.hub.search.util.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.piveau.hub.search.util.geo.BoundingBox;

import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchParams {

    private Date minDate;
    private Date maxDate;
    private BoundingBox boundingBox;

    public SearchParams() {
        this.minDate = null;
        this.maxDate = null;
        this.boundingBox = null;
    }

    public SearchParams(Date minDate, Date maxDate, BoundingBox boundingBox) {
        this.minDate = minDate;
        this.maxDate = maxDate;
        this.boundingBox = boundingBox;
    }

    public Date getMinDate() {
        return minDate;
    }

    public void setMinDate(Date minDate) {
        this.minDate = minDate;
    }

    public Date getMaxDate() {
        return maxDate;
    }

    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }

    public BoundingBox getBoundingBox() {
        return boundingBox;
    }

    public void setBoundingBox(BoundingBox boundingBox) {
        this.boundingBox = boundingBox;
    }

    @Override
    public String toString() {
        return "SearchParams{" +
                "minDate=" + minDate +
                ", maxDate=" + maxDate +
                ", boundingBox=" + boundingBox +
                '}';
    }
}
