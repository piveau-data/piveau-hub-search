package io.piveau.hub.search.services.search;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.connector.ElasticsearchConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class SearchServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) {
        JsonObject config = ConfigHelper.forConfig(config()).getJson(Constants.ENV_VIADUCT_HUB_SEARCH_ES_CONFIG);

        ElasticsearchConnector.create(vertx, config, connectorReady -> {
            if (connectorReady.succeeded()) {
                SearchService.create(connectorReady.result(), serviceReady -> {
                    if (serviceReady.succeeded()) {
                        new ServiceBinder(vertx).setAddress(SearchService.SERVICE_ADDRESS)
                                .register(SearchService.class, serviceReady.result());
                        startFuture.complete();
                    } else {
                        startFuture.fail(serviceReady.cause());
                    }
                });
            } else {
                startFuture.fail(connectorReady.cause());
            }
        });
    }
}
