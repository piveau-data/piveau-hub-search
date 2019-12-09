package io.piveau.hub.search.util.geo;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class Spatial {

    private static JsonArray checkGeometries(JsonArray geometries) {
        JsonArray geometriesChecked = new JsonArray();
        for (Object value : geometries) {
            if (value instanceof  JsonObject) {
                JsonObject valueChecked = checkSpatial((JsonObject) value);

                if (valueChecked == null) {
                    return null;
                } else {
                    geometriesChecked.add(valueChecked);
                }
            } else {
                return null;
            }
        }
        return geometriesChecked;
    }

    public static JsonObject checkSpatial(JsonObject spatial) {
        if (spatial == null) {
            return null;
        }

        String type = spatial.getString("type");

        if (type == null) {
            return null;
        }

        // feature is not supported in elasticsearch => translate to geometry
        // feature collection is not supported in elasticsearch => translate to geometry collection
        switch (type.toLowerCase()) {
            case "feature":
                return checkSpatial(spatial.getJsonObject("geometry"));
            case "featurecollection": {
                JsonObject spatialChecked = new JsonObject();

                JsonArray features = spatial.getJsonArray("features");

                if (features == null || features.isEmpty()) return null;

                JsonArray geometriesChecked = checkGeometries(features);
                if (geometriesChecked == null) return null;

                spatialChecked.put("type", "GeometryCollection");
                spatialChecked.put("geometries", geometriesChecked);

                return spatialChecked;
            }
            case "geometrycollection": {
                JsonObject spatialChecked = new JsonObject();

                JsonArray geometries = spatial.getJsonArray("geometries");

                if (geometries == null || geometries.isEmpty()) return null;

                JsonArray geometriesChecked = checkGeometries(geometries);
                if (geometriesChecked == null) return null;

                spatialChecked.put("type", type);
                spatialChecked.put("geometries", geometriesChecked);

                return spatialChecked;
            }
            default: {
                JsonObject spatialChecked = new JsonObject();

                JsonArray coordinates = spatial.getJsonArray("coordinates");
                // JsonArray orientation = spatial.getJsonArray("orientation");

                if (coordinates == null) {
                    return null;
                }

                spatialChecked.put("type", type);
                spatialChecked.put("coordinates", coordinates);

                return spatialChecked;
            }
        }
    }
}
