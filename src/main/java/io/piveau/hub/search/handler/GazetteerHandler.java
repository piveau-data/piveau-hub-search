package io.piveau.hub.search.handler;

import io.piveau.hub.search.services.gazetteer.GazetteerService;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;

public class GazetteerHandler extends ContextHandler {

    private GazetteerService gazetteerService;

    public GazetteerHandler(Vertx vertx, String address) {
        gazetteerService = GazetteerService.createProxy(vertx, address);
    }

    public void autocomplete(RoutingContext context) {
        String q = context.request().params().get("q");
        gazetteerService.autocomplete(q, ar -> handleContext(context, ar));
    }
}
