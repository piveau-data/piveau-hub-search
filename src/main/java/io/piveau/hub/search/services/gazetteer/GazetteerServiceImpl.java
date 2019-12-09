package io.piveau.hub.search.services.gazetteer;

import io.piveau.hub.search.util.connector.GazetteerConnector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public class GazetteerServiceImpl implements GazetteerService {

    private GazetteerConnector connector;

    GazetteerServiceImpl(GazetteerConnector connector, Handler<AsyncResult<GazetteerService>> handler) {
        this.connector = connector;
        handler.handle(Future.succeededFuture(this));
    }

    @Override
    public GazetteerService autocomplete(String q, Handler<AsyncResult<JsonObject>> handler) {
        connector.autocomplete(q, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }
}
