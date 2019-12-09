package io.piveau.hub.search.services.datasets;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.connector.CatalogueConnector;
import io.piveau.hub.search.util.connector.DatasetConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class DatasetsServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) {
        JsonObject config = ConfigHelper.forConfig(config()).getJson(Constants.ENV_VIADUCT_HUB_SEARCH_ES_CONFIG);

        Future<DatasetConnector> datasetConnectorFuture = Future.future();
        DatasetConnector.create(vertx, config, datasetConnectorReady -> {
            if (datasetConnectorReady.succeeded()) {
                datasetConnectorFuture.complete(datasetConnectorReady.result());
            } else {
                datasetConnectorFuture.fail(datasetConnectorReady.cause());
            }
        });

        Future<CatalogueConnector> catalogueConnectorFuture = Future.future();
        CatalogueConnector.create(vertx, config, catalogueConnectorReady -> {
            if (catalogueConnectorReady.succeeded()) {
                catalogueConnectorFuture.complete(catalogueConnectorReady.result());
            } else {
                catalogueConnectorFuture.fail(catalogueConnectorReady.cause());
            }
        });

        CompositeFuture.all(datasetConnectorFuture, catalogueConnectorFuture).setHandler(ar -> {
            if (ar.succeeded()) {
                DatasetsService.create(datasetConnectorFuture.result(), catalogueConnectorFuture.result(),
                        serviceReady -> {
                            if (serviceReady.succeeded()) {
                                new ServiceBinder(vertx).setAddress(DatasetsService.SERVICE_ADDRESS)
                                        .register(DatasetsService.class, serviceReady.result());
                                startFuture.complete();
                            } else {
                                startFuture.fail(serviceReady.cause());
                            }
                        });
            } else {
                startFuture.fail(ar.cause());
            }
        });
    }
}
