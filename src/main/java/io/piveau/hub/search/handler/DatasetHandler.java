package io.piveau.hub.search.handler;

import io.piveau.hub.search.services.datasets.DatasetsService;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;

import java.util.List;

public class DatasetHandler extends ContextHandler {

    private DatasetsService datasetsService;

    public DatasetHandler(Vertx vertx, String address) {
        datasetsService = DatasetsService.createProxy(vertx, address);
    }

    public void createDataset(RoutingContext context) {
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            datasetsService.createDataset(context.getBodyAsJson(), ar -> handleContext(context, ar));
        } else {
            datasetsService.createDataset(context.getBodyAsJson(), ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void createOrUpdateDataset(RoutingContext context) {
        String id = context.request().getParam("id");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            datasetsService.createOrUpdateDataset(id, context.getBodyAsJson(), ar -> handleContext(context, ar));
        } else {
            datasetsService.createOrUpdateDataset(id, context.getBodyAsJson(), ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void modifyDataset(RoutingContext context) {
        String id = context.request().getParam("id");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            datasetsService.modifyDataset(id, context.getBodyAsJson(), ar -> handleContext(context, ar));
        } else {
            datasetsService.modifyDataset(id, context.getBodyAsJson(), ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void readDataset(RoutingContext context) {
        String id = context.request().getParam("id");
        datasetsService.readDataset(id, ar -> handleContext(context, ar));
    }

    public void deleteDataset(RoutingContext context) {
        String id = context.request().getParam("id");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            datasetsService.deleteDataset(id, ar -> handleContext(context, ar));
        } else {
            datasetsService.deleteDataset(id, ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void createDatasetBulk(RoutingContext context) {
        JsonArray datasets = context.getBodyAsJson().getJsonArray("datasets");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            datasetsService.createDatasetBulk(datasets, ar -> handleContext(context, ar));
        } else {
            datasetsService.createDatasetBulk(datasets, ar -> {});
            context.response().setStatusCode(202).end();
        }
    }

    public void createOrUpdateDatasetBulk(RoutingContext context) {
        JsonArray datasets = context.getBodyAsJson().getJsonArray("datasets");
        List<String> synchronous = context.queryParam("synchronous");
        if (synchronous.isEmpty() || synchronous.contains("true")) {
            datasetsService.createOrUpdateDatasetBulk(datasets, ar -> handleContext(context, ar));
        } else {
            datasetsService.createOrUpdateDatasetBulk(datasets, ar -> {});
            context.response().setStatusCode(202).end();
        }
    }
}
