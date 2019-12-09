package io.piveau.hub.search.services.catalogues;

import io.piveau.hub.search.util.connector.CatalogueConnector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public class CataloguesServiceImpl implements CataloguesService {

    private CatalogueConnector catalogueConnector;

    CataloguesServiceImpl(CatalogueConnector catalogueConnector, Handler<AsyncResult<CataloguesService>> handler) {
        this.catalogueConnector = catalogueConnector;
        handler.handle(Future.succeededFuture(this));
    }

    @Override
    public CataloguesService createCatalogue(JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        catalogueConnector.createCatalogue(payload, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public CataloguesService createOrUpdateCatalogue(String catalogueId, JsonObject payload,
                                                     Handler<AsyncResult<JsonObject>> handler) {
        catalogueConnector.createOrUpdateCatalogue(catalogueId, payload, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public CataloguesService modifyCatalogue(String catalogueId, JsonObject payload,
                                             Handler<AsyncResult<JsonObject>> handler) {
        catalogueConnector.modifyCatalogue(catalogueId, payload, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public CataloguesService readCatalogue(String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        catalogueConnector.readCatalogue(catalogueId, ar ->  {
            if (ar.succeeded()) {
                catalogueConnector.countDatasets(catalogueId, countDatasetsResult -> {
                    if (countDatasetsResult.succeeded()) {
                        ar.result().getJsonObject("result").put("count", countDatasetsResult.result());
                    } else {
                        ar.result().getJsonObject("result").putNull("count");
                    }
                    handler.handle(Future.succeededFuture(ar.result()));
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public CataloguesService deleteCatalogue(String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        catalogueConnector.deleteCatalogue(catalogueId, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

}
