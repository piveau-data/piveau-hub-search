package io.piveau.hub.search.services.gazetteer;

import io.piveau.hub.search.util.connector.GazetteerConnector;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface GazetteerService {
    String SERVICE_ADDRESS = "de.fhg.fokus.viaduct.hub.search.gazetteer.queue";

    static GazetteerService create(GazetteerConnector connector,
                                   Handler<AsyncResult<GazetteerService>> readyHandler) {
        return new GazetteerServiceImpl(connector, readyHandler);
    }

    static GazetteerService createProxy(Vertx vertx, String address) {
        return new GazetteerServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    GazetteerService autocomplete(String q, Handler<AsyncResult<JsonObject>> handler);
}
