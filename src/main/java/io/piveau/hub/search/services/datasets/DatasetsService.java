package io.piveau.hub.search.services.datasets;

import io.piveau.hub.search.util.connector.CatalogueConnector;
import io.piveau.hub.search.util.connector.DatasetConnector;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface DatasetsService {
    String SERVICE_ADDRESS = "de.fhg.fokus.viaduct.hub.search.datasets.queue";

    static DatasetsService create(DatasetConnector datasetConnector, CatalogueConnector catalogueConnector,
                                  Handler<AsyncResult<DatasetsService>> readyHandler) {
        return new DatasetsServiceImpl(datasetConnector, catalogueConnector, readyHandler);
    }

    static DatasetsService createProxy(Vertx vertx, String address) {
        return new DatasetsServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    DatasetsService createDataset(JsonObject payload, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService createOrUpdateDataset(String datasetId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService modifyDataset(String datasetId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService readDataset(String datasetId, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService deleteDataset(String datasetId, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService createDatasetBulk(JsonArray payload, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    DatasetsService createOrUpdateDatasetBulk(JsonArray payload, Handler<AsyncResult<JsonObject>> handler);
}
