package io.piveau.hub.search.services.catalogues;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.connector.CatalogueConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class CataloguesServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) {
        JsonObject config = ConfigHelper.forConfig(config()).getJson(Constants.ENV_VIADUCT_HUB_SEARCH_ES_CONFIG);

        CatalogueConnector.create(vertx, config, catalogueConnectorReady -> {
            if (catalogueConnectorReady.succeeded()) {
                CataloguesService.create(catalogueConnectorReady.result(), serviceReady -> {
                    if (serviceReady.succeeded()) {
                        new ServiceBinder(vertx).setAddress(CataloguesService.SERVICE_ADDRESS)
                                .register(CataloguesService.class, serviceReady.result());
                        startFuture.complete();
                    } else {
                        startFuture.fail(serviceReady.cause());
                    }
                });
            } else {
                startFuture.fail(catalogueConnectorReady.cause());
            }
        });
    }
}
