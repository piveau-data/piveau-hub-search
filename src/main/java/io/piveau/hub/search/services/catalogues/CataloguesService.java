package io.piveau.hub.search.services.catalogues;

import io.piveau.hub.search.util.connector.CatalogueConnector;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface CataloguesService {
    String SERVICE_ADDRESS = "de.fhg.fokus.viaduct.hub.search.catalogues.queue";

    static CataloguesService create(CatalogueConnector catalogueConnector,
                                    Handler<AsyncResult<CataloguesService>> readyHandler) {
        return new CataloguesServiceImpl(catalogueConnector, readyHandler);
    }

    static CataloguesService createProxy(Vertx vertx, String address) {
        return new CataloguesServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    CataloguesService createCatalogue(JsonObject payload, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    CataloguesService createOrUpdateCatalogue(String catalogueId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    CataloguesService modifyCatalogue(String catalogueId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    CataloguesService readCatalogue(String catalogueId, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    CataloguesService deleteCatalogue(String catalogueId, Handler<AsyncResult<JsonObject>> handler);

}
