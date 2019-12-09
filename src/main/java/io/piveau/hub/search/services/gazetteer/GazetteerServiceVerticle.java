package io.piveau.hub.search.services.gazetteer;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.connector.GazetteerConnector;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class GazetteerServiceVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) {
        JsonObject config = ConfigHelper.forConfig(config()).getJson(Constants.ENV_VIADUCT_HUB_SEARCH_GAZETTEER_CONFIG);

        GazetteerConnector.create(vertx, config, connectorReady -> {
            if (connectorReady.succeeded()) {
                GazetteerService.create(connectorReady.result(), serviceReady -> {
                    if (serviceReady.succeeded()) {
                        new ServiceBinder(vertx).setAddress(GazetteerService.SERVICE_ADDRESS)
                                .register(GazetteerService.class, serviceReady.result());
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
