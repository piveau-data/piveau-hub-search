package io.piveau.hub.search.handler;

import io.piveau.hub.search.services.catalogues.CataloguesService;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;

import java.util.List;

public class CatalogueHandler extends ContextHandler {

    private CataloguesService cataloguesService;

    public CatalogueHandler(Vertx vertx, String address) {
        cataloguesService = CataloguesService.createProxy(vertx, address);
    }

    public void createCatalogue(RoutingContext context) {
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            cataloguesService.createCatalogue(context.getBodyAsJson(), ar -> handleContext(context, ar));
        } else {
            cataloguesService.createCatalogue(context.getBodyAsJson(), ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void createOrUpdateCatalogue(RoutingContext context) {
        String id = context.request().getParam("id");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            cataloguesService.createOrUpdateCatalogue(id, context.getBodyAsJson(), ar -> handleContext(context, ar));
        } else {
            cataloguesService.createOrUpdateCatalogue(id, context.getBodyAsJson(), ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void modifyCatalogue(RoutingContext context) {
        String id = context.request().getParam("id");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            cataloguesService.modifyCatalogue(id, context.getBodyAsJson(), ar -> handleContext(context, ar));
        } else {
            cataloguesService.modifyCatalogue(id, context.getBodyAsJson(), ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void readCatalogue(RoutingContext context) {
        String id = context.request().getParam("id");
        cataloguesService.readCatalogue(id, ar -> handleContext(context, ar));
    }

    public void deleteCatalogue(RoutingContext context) {
        String id = context.request().getParam("id");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            cataloguesService.deleteCatalogue(id, ar -> handleContext(context, ar));
        } else {
            cataloguesService.deleteCatalogue(id, ar -> {});
            context.response().setStatusCode(202).end();
        }
    }
}
