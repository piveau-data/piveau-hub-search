package io.piveau.hub.search.handler;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class ApiKeyHandler {

    private final String apiKey;

    public ApiKeyHandler(String api_key) {
        this.apiKey = api_key;
    }

    public void checkApiKey(RoutingContext context) {

        final String authorization = context.request().headers().get(HttpHeaders.AUTHORIZATION);

        if(this.apiKey.isEmpty()) {
            JsonObject response = new JsonObject();
            context.response().putHeader("Content-Type", "application/json");
            context.response().setStatusCode(500);
            response.put("success", false);
            response.put("message", "Api-Key is not specified");
        } else if(authorization == null) {
            JsonObject response = new JsonObject();
            context.response().putHeader("Content-Type", "application/json");
            context.response().setStatusCode(401);
            response.put("success", false);
            response.put("message", "Header field Authorization is missing");
            context.response().end(response.toString());
        } else if (!authorization.equals(this.apiKey)) {
            JsonObject response = new JsonObject();
            context.response().putHeader("Content-Type", "application/json");
            context.response().setStatusCode(401);
            response.put("success", false);
            response.put("message", "Incorrect Api-Key");
            context.response().end(response.toString());
        } else {
            context.next();
        }
    }
}
