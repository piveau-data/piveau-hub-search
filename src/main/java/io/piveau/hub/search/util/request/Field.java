package io.piveau.hub.search.util.request;

import java.util.List;

public class Field {

    private String name;
    private Float boost;
    private List<Field> subFields;
    private Boolean searchable;
    private String type;

    public Field(String name, String type) {
        this.name = name;
        this.boost = 1.0f;
        this.subFields = null;
        this.searchable = false;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Float getBoost() {
        return boost;
    }

    public void setBoost(Float boost) {
        this.boost = boost;
    }

    public List<Field> getSubFields() {
        return subFields;
    }

    public void setSubFields(List<Field> subFields) {
        this.subFields = subFields;
    }

    public Boolean isSearchable() {
        return searchable;
    }

    public void setSearchable(Boolean searchable) {
        this.searchable = searchable;
    }

    public Boolean getSearchable() {
        return searchable;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", boost=" + boost +
                ", subFields=" + subFields +
                ", searchable=" + searchable +
                ", type=" + type +
                '}';
    }
}
