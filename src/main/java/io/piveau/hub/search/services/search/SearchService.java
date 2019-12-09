package io.piveau.hub.search.services.search;

import io.piveau.hub.search.util.connector.ElasticsearchConnector;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface SearchService {
    String SERVICE_ADDRESS = "de.fhg.fokus.viaduct.hub.search.search.queue";

    static SearchService create(ElasticsearchConnector connector,
                                Handler<AsyncResult<SearchService>> readyHandler) {
        return new SearchServiceImpl(connector, readyHandler);
    }

    static SearchService createProxy(Vertx vertx, String address) {
        return new SearchServiceVertxEBProxy(vertx, address);
    }

    @Fluent
    SearchService search(String q, Handler<AsyncResult<JsonObject>> handler);

    @Fluent
    SearchService indexCreate(String index, Handler<AsyncResult<String>> handler);

    @Fluent
    SearchService indexDelete(String index, Handler<AsyncResult<String>> handler);

    @Fluent
    SearchService mapping(String index, Handler<AsyncResult<String>> handler);

    @Fluent
    SearchService boost(String index, String field, Float value, Handler<AsyncResult<String>> handler);

    @Fluent
    SearchService setMaxAggSize(String index, Integer max_agg_size, Handler<AsyncResult<String>> handler);

    @Fluent
    SearchService setMaxResultWindow(String index, Integer max_result_window, Handler<AsyncResult<String>> handler);

}
