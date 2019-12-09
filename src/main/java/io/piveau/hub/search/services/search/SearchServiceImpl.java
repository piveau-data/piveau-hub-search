package io.piveau.hub.search.services.search;

import io.piveau.hub.search.util.connector.ElasticsearchConnector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public class SearchServiceImpl implements SearchService {

    private ElasticsearchConnector connector;

    SearchServiceImpl(ElasticsearchConnector connector, Handler<AsyncResult<SearchService>> handler) {
        this.connector = connector;
        handler.handle(Future.succeededFuture(this));
    }

    @Override
    public SearchService search(String q, Handler<AsyncResult<JsonObject>> handler) {
        connector.search(q, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public SearchService indexCreate(String index, Handler<AsyncResult<String>> handler) {
        connector.indexCreate(index, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public SearchService indexDelete(String index, Handler<AsyncResult<String>> handler) {
        connector.indexDelete(index, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public SearchService mapping(String index, Handler<AsyncResult<String>> handler) {
        connector.putMapping(index, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public SearchService boost(String index, String field, Float value, Handler<AsyncResult<String>> handler) {
        connector.getIndexManager().boost(index, field, value, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public SearchService setMaxAggSize(String index, Integer max_agg_size, Handler<AsyncResult<String>> handler) {
        connector.getIndexManager().setMaxAggSize(index, max_agg_size, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }

    @Override
    public SearchService setMaxResultWindow(String index, Integer max_result_window, Handler<AsyncResult<String>> handler) {
        connector.setMaxResultWindow(index, max_result_window, ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
        return this;
    }
}
