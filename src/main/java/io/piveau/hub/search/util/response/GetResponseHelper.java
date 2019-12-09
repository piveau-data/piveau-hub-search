package io.piveau.hub.search.util.response;

import io.piveau.hub.search.util.request.Field;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.document.DocumentField;

import java.util.Map;

public class GetResponseHelper {

    public static JsonObject getResponseToJson(GetResponse getResponse, Map<String, Field> fields) {
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();

        JsonObject result;
        if (fields == null) {
            result = new JsonObject(sourceAsMap);
        } else {
            result = new JsonObject();
            for (String field : fields.keySet()) {
                if (getResponse.getIndex().equals("dataset") && field.equals("distributions")
                        && sourceAsMap.get("distributions") == null) {
                    result.put("distributions", new JsonArray());
                } else {
                    result.put(field, sourceAsMap.get(field));
                }
            }
        }

        DocumentField doc = getResponse.getFields().get("_ignored");

        if (doc != null) {
            for (Object value : doc.getValues()) {
                result.remove(value.toString());
            }
        }

        return result;
    }
}
