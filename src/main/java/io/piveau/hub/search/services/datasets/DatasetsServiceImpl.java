package io.piveau.hub.search.services.datasets;

import io.piveau.hub.search.util.connector.CatalogueConnector;
import io.piveau.hub.search.util.connector.DatasetConnector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DatasetsServiceImpl implements DatasetsService {

    private DatasetConnector datasetConnector;
    private CatalogueConnector catalogueConnector;

    DatasetsServiceImpl(DatasetConnector datasetConnector, CatalogueConnector catalogueConnector,
                        Handler<AsyncResult<DatasetsService>> handler) {
        this.datasetConnector = datasetConnector;
        this.catalogueConnector = catalogueConnector;
        handler.handle(Future.succeededFuture(this));
    }

    @Override
    public DatasetsService createDataset(JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        datasetConnector.checkSpatial(payload, checkSpatialResult ->
            catalogueConnector.checkCatalogInPayload(payload, ar -> {
                if (ar.succeeded()) {
                    datasetConnector.createDataset(payload, createDatasetResult ->  {
                        if (createDatasetResult.succeeded()) {
                            handler.handle(Future.succeededFuture(createDatasetResult.result()));
                        } else {
                            handler.handle(Future.failedFuture(createDatasetResult.cause()));
                        }
                    });
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            })
        );
        return this;
    }

    @Override
    public DatasetsService createOrUpdateDataset(String datasetId, JsonObject payload,
                                                 Handler<AsyncResult<JsonObject>> handler) {
        datasetConnector.checkSpatial(payload, checkSpatialResult ->
            catalogueConnector.checkCatalogInPayload(payload, ar -> {
                if (ar.succeeded()) {
                    datasetConnector.createOrUpdateDataset(datasetId, payload,
                            createOrUpdateDatasetResult ->  {
                                if (createOrUpdateDatasetResult.succeeded()) {
                                    handler.handle(Future.succeededFuture(createOrUpdateDatasetResult.result()));
                                } else {
                                    handler.handle(Future.failedFuture(createOrUpdateDatasetResult.cause()));
                                }
                            });
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            })
        );
        return this;
    }

    @Override
    public DatasetsService modifyDataset(String datasetId, JsonObject payload,
                                         Handler<AsyncResult<JsonObject>> handler) {
        datasetConnector.checkSpatial(payload, checkSpatialResult ->
            catalogueConnector.checkCatalogInPayload(payload, ar -> {
                if (ar.succeeded()) {
                    datasetConnector.modifyDataset(datasetId, payload,
                            modifyDatasetResult ->  {
                                if (modifyDatasetResult.succeeded()) {
                                    handler.handle(Future.succeededFuture(modifyDatasetResult.result()));
                                } else {
                                    handler.handle(Future.failedFuture(modifyDatasetResult.cause()));
                                }
                            });
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            })
        );
        return this;
    }

    @Override
    public DatasetsService readDataset(String datasetId, Handler<AsyncResult<JsonObject>> handler) {
        datasetConnector.readDataset(datasetId, ar ->  {
            if (ar.succeeded()) {
                catalogueConnector.replaceCatalogInResponse(ar.result().getJsonObject("result"),
                        replaceCatalogInResponseResult -> {
                    if (replaceCatalogInResponseResult.succeeded()) {
                        handler.handle(Future.succeededFuture(ar.result()));
                    } else {
                        handler.handle(Future.failedFuture(replaceCatalogInResponseResult.cause()));
                    }
                });

            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DatasetsService deleteDataset(String datasetId, Handler<AsyncResult<JsonObject>> handler) {
        datasetConnector.deleteDataset(datasetId, ar ->  {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public DatasetsService createDatasetBulk(JsonArray payload, Handler<AsyncResult<JsonObject>> handler) {
        datasetConnector.checkSpatial(payload, checkSpatialResult ->
            catalogueConnector.checkCatalogInPayload(payload, ar ->
                datasetConnector.datasetBulk(payload, ar.result(), "create", datasetBulkResult -> {
                    if (datasetBulkResult.succeeded()) {
                        handler.handle(Future.succeededFuture(datasetBulkResult.result()));
                    } else {
                        handler.handle(Future.failedFuture(datasetBulkResult.cause()));
                    }
                }))
        );
        return this;
    }

    @Override
    public DatasetsService createOrUpdateDatasetBulk(JsonArray payload, Handler<AsyncResult<JsonObject>> handler) {
        datasetConnector.checkSpatial(payload, checkSpatialResult ->
            catalogueConnector.checkCatalogInPayload(payload, ar ->
                datasetConnector.datasetBulk(payload, ar.result(), "replace", datasetBulkResult -> {
                    if (datasetBulkResult.succeeded()) {
                        handler.handle(Future.succeededFuture(datasetBulkResult.result()));
                    } else {
                        handler.handle(Future.failedFuture(datasetBulkResult.cause()));
                    }
                }))
        );
        return this;
    }

}
