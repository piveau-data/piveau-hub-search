package io.piveau.hub.search;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.services.catalogues.CataloguesService;
import io.piveau.hub.search.services.catalogues.CataloguesServiceVerticle;
import io.piveau.hub.search.services.datasets.DatasetsService;
import io.piveau.hub.search.services.datasets.DatasetsServiceVerticle;
import io.piveau.hub.search.services.gazetteer.GazetteerService;
import io.piveau.hub.search.services.gazetteer.GazetteerServiceVerticle;
import io.piveau.hub.search.services.search.SearchService;
import io.piveau.hub.search.services.search.SearchServiceVerticle;
import io.piveau.hub.search.verticles.ShellVerticle;
import io.piveau.hub.search.handler.*;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.api.contract.RouterFactoryOptions;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.ext.web.api.validation.ValidationException;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MainVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);

    private DatasetHandler datasetHandler;
    private CatalogueHandler catalogueHandler;
    // private DistributionHandler distributionHandler;
    private SearchHandler searchHandler;
    private ApiKeyHandler apiKeyHandler;
    private GazetteerHandler gazetteerHandler;
    private CkanHandler ckanHandler;
    private FeedHandler feedHandler;
    private SitemapHandler sitemapHandler;

    @Override
    public void start(Future<Void> startFuture) {
        Future<Void> steps = loadConfig()
                .compose(this::bootstrapVerticles)
                .compose(this::startServer);

        steps.setHandler(handler -> {
            if (handler.succeeded()) {
                LOG.info("Successfully launched hub-search");
                startFuture.complete();
            } else {
                LOG.error("Failed to launch hub-search: " + handler.cause());
                startFuture.fail(handler.cause());
            }
        });
    }

    private Future<Void> startServer(JsonObject config) {
        Future<Void> future = Future.future();

        Integer service_port = config.getInteger(Constants.ENV_VIADUCT_HUB_SEARCH_SERVICE_PORT, 8080);
        String api_key = config.getString(Constants.ENV_VIADUCT_HUB_SEARCH_API_KEY);

        if(api_key == null) {
            LOG.error("No api_key specified");
            future.fail("No api_key specified");
            return future;
        }

        Set<String> allowedHeaders = new HashSet<>();
        allowedHeaders.add("x-requested-with");
        allowedHeaders.add("Access-Control-Allow-Origin");
        allowedHeaders.add("origin");
        allowedHeaders.add("Content-Type");
        allowedHeaders.add("accept");

        Set<HttpMethod> allowedMethods = new HashSet<>();
        allowedMethods.add(HttpMethod.GET);
        allowedMethods.add(HttpMethod.POST);
        allowedMethods.add(HttpMethod.OPTIONS);
        allowedMethods.add(HttpMethod.DELETE);
        allowedMethods.add(HttpMethod.PATCH);
        allowedMethods.add(HttpMethod.PUT);

        OpenAPI3RouterFactory.create(vertx, "webroot/openapi.yaml", handler -> {
            if (handler.succeeded()) {
                OpenAPI3RouterFactory routerFactory = handler.result();
                RouterFactoryOptions options = new RouterFactoryOptions()
                        .setMountNotImplementedHandler(true)
                        .setRequireSecurityHandlers(true);

                routerFactory.setOptions(options);

                apiKeyHandler = new ApiKeyHandler(api_key);
                routerFactory.addSecurityHandler("ApiKeyAuth", apiKeyHandler::checkApiKey);

                routerFactory.addHandlerByOperationId("createDataset", datasetHandler::createDataset);
                routerFactory.addHandlerByOperationId("createOrUpdateDataset", datasetHandler::createOrUpdateDataset);
                routerFactory.addHandlerByOperationId("modifyDataset", datasetHandler::modifyDataset);
                routerFactory.addHandlerByOperationId("readDataset", datasetHandler::readDataset);
                routerFactory.addHandlerByOperationId("deleteDataset", datasetHandler::deleteDataset);
                routerFactory.addHandlerByOperationId("createDatasetBulk", datasetHandler::createDatasetBulk);
                routerFactory.addHandlerByOperationId("createOrUpdateDatasetBulk", datasetHandler::createOrUpdateDatasetBulk);

                routerFactory.addHandlerByOperationId("createCatalogue", catalogueHandler::createCatalogue);
                routerFactory.addHandlerByOperationId("createOrUpdateCatalogue", catalogueHandler::createOrUpdateCatalogue);
                routerFactory.addHandlerByOperationId("modifyCatalogue", catalogueHandler::modifyCatalogue);
                routerFactory.addHandlerByOperationId("readCatalogue", catalogueHandler::readCatalogue);
                routerFactory.addHandlerByOperationId("deleteCatalogue", catalogueHandler::deleteCatalogue);

                routerFactory.addHandlerByOperationId("searchGet", searchHandler::searchGet);
                routerFactory.addHandlerByOperationId("searchPost", searchHandler::searchPost);
                routerFactory.addHandlerByOperationId("searchAutocomplete", searchHandler::searchAutocomplete);

                routerFactory.addHandlerByOperationId("gazetteerAutocomplete", gazetteerHandler::autocomplete);

                routerFactory.addHandlerByOperationId("ckanPackageSearch", ckanHandler::package_search);
                routerFactory.addHandlerByOperationId("ckanPackageShow", ckanHandler::package_show);

                routerFactory.addHandlerByOperationId("datasets.atom", feedHandler::atom);
                routerFactory.addHandlerByOperationId("datasets.rss", feedHandler::rss);

                routerFactory.addHandlerByOperationId("sitemapMeta", sitemapHandler::getMeta);
                routerFactory.addHandlerByOperationId("sitemapDatasets", sitemapHandler::getDatasets);

                Router router = routerFactory.getRouter();
                router.route().handler(StaticHandler.create());
                router.route().handler(
                        CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods)
                );
                router.errorHandler(400, context -> {
                    Throwable failure = context.failure();
                    if (failure instanceof ValidationException) {
                        LOG.debug(failure.getMessage());
                        context.response().putHeader("Content-Type", "application/json");
                        JsonObject response = new JsonObject();
                        response.put("status", "error");
                        response.put("message", failure.getMessage());
                        context.response().setStatusCode(400);
                        context.response().end(response.encode());
                    }
                });

                HttpServer server = vertx.createHttpServer(new HttpServerOptions().setPort(service_port));
                server.requestHandler(router).listen((ar) ->  {
                    if (ar.succeeded()) {
                        LOG.info("Successfully launched server on port [{}]", service_port);
                        future.complete();
                    } else {
                        LOG.error("Failed to start server at [{}]: {}", service_port, handler.cause());
                        future.fail(ar.cause());
                    }
                });
            } else {
                // Something went wrong during router factory initialization
                LOG.error("Failed to start server at [{}]: {}", service_port, handler.cause());
                future.fail(handler.cause());
            }
        });

        return future;
    }

    private Future<JsonObject> loadConfig() {
        Future<JsonObject> future = Future.future();

        ConfigStoreOptions envStoreOptions = new ConfigStoreOptions()
                .setType("env")
                .setConfig(new JsonObject().put("keys", new JsonArray()
                        .add(Constants.ENV_VIADUCT_HUB_SEARCH_SERVICE_PORT)
                        .add(Constants.ENV_VIADUCT_HUB_SEARCH_BOOST)
                        .add(Constants.ENV_VIADUCT_HUB_SEARCH_API_KEY)
                        .add(Constants.ENV_VIADUCT_HUB_SEARCH_ES_CONFIG)
                        .add(Constants.ENV_VIADUCT_HUB_SEARCH_CLI_CONFIG)
                        .add(Constants.ENV_VIADUCT_HUB_SEARCH_GAZETTEER_CONFIG)
                        .add(Constants.ENV_VIADUCT_HUB_SEARCH_TITLE)
                        .add(Constants.ENV_VIADUCT_HUB_SEARCH_SITEMAP_CONFIG)
                ));

        ConfigStoreOptions fileStoreOptions = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "conf/config.json"));

        ConfigRetriever.create(vertx, new ConfigRetrieverOptions()
                .addStore(fileStoreOptions)
                .addStore(envStoreOptions)).getConfig(handler -> {
            if (handler.succeeded()) {
                LOG.info(handler.result().encodePrettily());
                future.complete(handler.result());
            } else {
                future.failed();
            }
        });

        return future;
    }

    private Future<JsonObject> bootstrapVerticles(JsonObject config) {
        Future<JsonObject> future = Future.future();

        Future<String> shellFuture = Future.future();
        vertx.deployVerticle(ShellVerticle.class.getName(), new DeploymentOptions()
                .setConfig(config).setWorker(true)/*.setInstances(4)*/, shellFuture);

        Future<String> gazetteerFuture = Future.future();
        vertx.deployVerticle(GazetteerServiceVerticle.class.getName(), new DeploymentOptions()
                .setConfig(config).setWorker(true)/*.setInstances(4)*/, gazetteerFuture);

        Future<String> datasetsFuture = Future.future();
        vertx.deployVerticle(DatasetsServiceVerticle.class.getName(), new DeploymentOptions()
                .setConfig(config).setWorker(true)/*.setInstances(4)*/, datasetsFuture);

        Future<String> cataloguesFuture = Future.future();
        vertx.deployVerticle(CataloguesServiceVerticle.class.getName(), new DeploymentOptions()
                .setConfig(config).setWorker(true)/*.setInstances(4)*/, cataloguesFuture);

        Future<String> searchServiceVerticle = Future.future();
        vertx.deployVerticle(SearchServiceVerticle.class.getName(), new DeploymentOptions()
                        .setConfig(config).setWorker(true)/*.setInstances(4)*/, searchServiceVerticle);

        CompositeFuture.all(Arrays.asList(shellFuture, gazetteerFuture, datasetsFuture, cataloguesFuture,
                searchServiceVerticle)).setHandler(ar -> {
            if (ar.succeeded()) {
                datasetHandler = new DatasetHandler(vertx, DatasetsService.SERVICE_ADDRESS);
                catalogueHandler = new CatalogueHandler(vertx, CataloguesService.SERVICE_ADDRESS);
                // distributionHandler = new DistributionHandler(vertx, DocumentsService.SERVICE_ADDRESS);
                gazetteerHandler = new GazetteerHandler(vertx, GazetteerService.SERVICE_ADDRESS);
                searchHandler = new SearchHandler(vertx, SearchService.SERVICE_ADDRESS);
                ckanHandler = new CkanHandler(vertx, SearchService.SERVICE_ADDRESS, DatasetsService.SERVICE_ADDRESS);
                feedHandler = new FeedHandler(config.getString(Constants.ENV_VIADUCT_HUB_SEARCH_TITLE, ""), vertx,
                        SearchService.SERVICE_ADDRESS);
                sitemapHandler = new SitemapHandler(config.getJsonObject(Constants.ENV_VIADUCT_HUB_SEARCH_SITEMAP_CONFIG),
                        vertx, SearchService.SERVICE_ADDRESS);
                future.complete(config);
            } else {
                future.fail(ar.cause());
            }
        });

        return future;
    }
}
