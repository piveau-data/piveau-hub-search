package io.piveau.hub.search.util.connector;

import io.piveau.hub.search.util.index.IndexManager;
import io.piveau.hub.search.util.response.GetResponseHelper;
import io.piveau.hub.search.util.response.ReturnHelper;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class CatalogueConnector {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogueConnector.class);

    // elasticsearch client
    private RestHighLevelClient client;

    // index manager
    private IndexManager indexManager;

    public static CatalogueConnector create(Vertx vertx, JsonObject config, Handler<AsyncResult<CatalogueConnector>> handler) {
        return new CatalogueConnector(vertx, config, handler);
    }

    private CatalogueConnector(Vertx vertx, JsonObject config, Handler<AsyncResult<CatalogueConnector>> handler) {
        String host = config.getString("host", "localhost");
        Integer port = config.getInteger("port", 9200);

        this.client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, "http"))
        );

        IndexManager.create(vertx, config, ar -> {
            if (ar.succeeded()) {
                this.indexManager = ar.result();

                handler.handle(Future.succeededFuture(this));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void replaceCatalogInResponse(JsonObject response, Handler<AsyncResult> handler) {
        JsonObject catalog = response.getJsonObject("catalog");
        if (catalog != null && !catalog.isEmpty()) {
            String catalogueId = response.getJsonObject("catalog").getString("id");
            readCatalogue(catalogueId, ar -> {
                if (ar.succeeded()) {
                    JsonObject readCatalog = ar.result().getJsonObject("result");

                    response.put("catalog", readCatalog);
                    response.put("country", readCatalog.getJsonObject("country"));

                    handler.handle(Future.succeededFuture());
                } else {
                    handler.handle(Future.failedFuture(ar.cause().getMessage()));
                }
            });
        } else {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "Catalog missing")));
        }
    }

    public void checkCatalogInPayload(JsonArray payload,
                                        Handler<AsyncResult<CopyOnWriteArrayList<JsonObject>>> handler) {
        List<Future> futures = new ArrayList<>();

        CopyOnWriteArrayList<JsonObject> result = new CopyOnWriteArrayList<>();

        for (Object value : payload) {
            Future<Object> future = Future.future();
            futures.add(future);

            JsonObject valueJson = (JsonObject) value;
            checkCatalogInPayload(valueJson, ar -> {
                if (ar.succeeded()) {
                    future.complete(null);
                } else {
                    JsonObject failure = new JsonObject(ar.cause().getMessage());

                    result.add(new JsonObject()
                            .put("success", false)
                            .put("status", failure.getInteger("status"))
                            .put("message", failure.getString("message"))
                            .put("id", valueJson.getString("id"))
                    );

                    future.complete(value);
                }
            });
        }

        CompositeFuture.all(futures).setHandler(ar -> {
            for (Future future : futures) {
                if (future.result() != null) {
                    payload.remove(future.result());
                }
            }

            handler.handle(Future.succeededFuture(result));
        });
    }

    public void checkCatalogInPayload(JsonObject payload, Handler<AsyncResult> handler) {
        JsonObject catalog = payload.getJsonObject("catalog");
        if (catalog != null && !catalog.isEmpty()) {
            String catalogueId = payload.getJsonObject("catalog").getString("id");
            readCatalogue(catalogueId, ar -> {
                if (ar.succeeded()) {
                    JsonObject readCatalog = ar.result().getJsonObject("result");

                    payload.put("catalog", new JsonObject().put("id", catalogueId));
                    payload.put("country", readCatalog.getJsonObject("country"));

                    handler.handle(Future.succeededFuture());
                } else {
                    handler.handle(Future.failedFuture(ar.cause().getMessage()));
                }
            });
        } else {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "Catalog missing")));
        }
    }

    public void createCatalogue(JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        if (payload == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is null")));
            return;
        }

        String catalogueId = UUID.randomUUID().toString();

        payload.put("id", catalogueId);

        JsonObject result = new JsonObject().put("id", catalogueId);

        IndexRequest indexRequest = new IndexRequest("catalogue").id(catalogueId).opType("create")
                .source(payload.encode(), XContentType.JSON);
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                LOG.info("Index catalogue: Catalogue {} created. {}", catalogueId, indexResponse.toString());
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(201, result)));
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Index catalogue: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                            statusException.getMessage())));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                }
            }
        });
    }

    private void updateByQuery(String catalogueId, JsonObject payload, boolean replaceAll,
                               Handler<AsyncResult<BulkByScrollResponse>> handler) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest("dataset");
        updateByQueryRequest.setConflicts("proceed");

        updateByQueryRequest.setQuery(new TermQueryBuilder("catalog.id.raw", catalogueId));

        StringBuilder script = new StringBuilder();

        if (payload.getJsonObject("country") != null) {
            script.append("ctx._source.country = params.catalog_country;");
        }

        Map<String, Object> params = new HashMap<>();

        if (replaceAll) {
            Map<String, Object> map = new HashMap<>();
            for (Map.Entry<String, Object> entry : payload.getMap().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (value.getClass().equals(JsonArray.class)) {
                    map.putIfAbsent(key, ((JsonArray) value).getList());
                } else if (value.getClass().equals(JsonObject.class)) {
                    map.putIfAbsent(key, ((JsonObject) value).getMap());
                } else {
                    map.putIfAbsent(key, value);
                }
            }

            // script.append("ctx._source.catalog = params.catalog_catalog;");

            params.putIfAbsent("catalog_country", payload.getJsonObject("country").getMap());
            params.putIfAbsent("catalog_catalog", map);
        } else {
            for (Map.Entry<String, Object> entry : payload.getMap().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                /*script.append(" ctx._source.catalog.");
                script.append(key);
                script.append(" = params.catalog_");
                script.append(key);
                script.append(";");*/

                // LOG.debug(entry.getValue().getClass().toString());
                // LOG.debug(key);

                if (value.getClass().equals(JsonArray.class)) {
                    params.putIfAbsent("catalog_" + key, ((JsonArray) value).getList());
                } else if (value.getClass().equals(JsonObject.class)) {
                    params.putIfAbsent("catalog_" + key, ((JsonObject) value).getMap());
                } else {
                    params.putIfAbsent("catalog_" + key, value);
                }
            }
        }

        updateByQueryRequest.setScript(
                new Script(
                        ScriptType.INLINE,
                        "painless",
                        script.toString(),
                        params
                )
        );

        client.updateByQueryAsync(updateByQueryRequest, RequestOptions.DEFAULT,
                new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse bulkResponse) {
                        LOG.info("Index catalogue: Catalogue {} updated all datasets inside.", catalogueId);
                        handler.handle(Future.succeededFuture(bulkResponse));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        e.printStackTrace();
                        LOG.error("Index catalogue: " + e);
                        handler.handle(Future.failedFuture(e));
                    }
                });
    }

    public void createOrUpdateCatalogue(String catalogueId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        if (catalogueId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (catalogueId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        if (payload == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is null")));
            return;
        }

        payload.put("id", catalogueId);

        JsonObject result = new JsonObject().put("id", catalogueId);

        IndexRequest indexRequest = new IndexRequest("catalogue").id(catalogueId)
                .source(payload.encode(), XContentType.JSON);
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                if (indexResponse.status().getStatus() == 200) {
                    // updated
                    LOG.info("Index catalogue: Catalogue {} updated. {}", catalogueId, indexResponse.toString());
                    updateByQuery(catalogueId, payload, true, ar -> {});
                } else {
                    // created
                    LOG.info("Index catalogue: Catalogue {} created. {}", catalogueId, indexResponse.toString());
                }
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(indexResponse.status().getStatus(), result)));
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Index catalogue: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                            statusException.getMessage())));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                }
            }
        });
    }

    public void modifyCatalogue(String catalogueId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        if (catalogueId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (catalogueId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        if (payload == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is null")));
            return;
        }

        JsonObject result = new JsonObject().put("id", catalogueId);

        UpdateRequest updateRequest = new UpdateRequest("catalogue", catalogueId)
                .doc(payload.toString(), XContentType.JSON);
        client.updateAsync(updateRequest, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                LOG.info("Index catalogue: Catalogue {} modified. {}", catalogueId, updateResponse.toString());
                updateByQuery(catalogueId, payload, false, ar -> {});
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, result)));
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Index catalogue: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                            statusException.getMessage())));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                }
            }
        });
    }

    public void readCatalogue(String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        if (catalogueId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (catalogueId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        GetRequest getRequest = new GetRequest("catalogue", catalogueId);
        client.getAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()) {
                    handler.handle(Future.succeededFuture(
                            ReturnHelper.returnSuccess(200, GetResponseHelper.getResponseToJson(getResponse,
                                    indexManager.getFields().get("catalogue")))));
                } else {
                    LOG.error("Read catalogue: Catalogue {} not found", catalogueId);
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(404,
                            "Catalogue " + catalogueId + " not found")));
                }
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Read catalogue: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                            statusException.getMessage())));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                }
            }
        });
    }

    public void deleteCatalogue(String catalogueId, Handler<AsyncResult<JsonObject>> handler) {
        if (catalogueId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (catalogueId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        JsonObject result = new JsonObject().put("id", catalogueId);

        DeleteRequest deleteRequest = new DeleteRequest("catalogue", catalogueId);
        client.deleteAsync(deleteRequest, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.status() == RestStatus.NOT_FOUND) {
                    LOG.error("Delete catalogue: Catalogue {} not found", catalogueId);
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(404,
                            "Catalogue " + catalogueId + " not found")));
                } else {
                    DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest("dataset");
                    deleteByQueryRequest.setConflicts("proceed");
                    deleteByQueryRequest.setQuery(new TermQueryBuilder("catalog.id.raw", catalogueId));
                    client.deleteByQueryAsync(deleteByQueryRequest, RequestOptions.DEFAULT,
                            new ActionListener<BulkByScrollResponse>() {
                                @Override
                                public void onResponse(BulkByScrollResponse bulkResponse) {
                                    LOG.info("Delete catalogue: Catalogue {} deleted all datasets inside.",
                                            catalogueId);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    LOG.error("Delete catalogue: " + e);
                                }
                            });

                    handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, result)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Delete catalogue: " + e);
                if (e.getClass().equals(ElasticsearchStatusException.class)) {
                    ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                            statusException.getMessage())));
                } else {
                    handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                }
            }
        });
    }

    public void countDatasets(String query, Handler<AsyncResult<Long>> handler) {
        QueryBuilder termQuery = QueryBuilders.termQuery("catalog.id", query);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(termQuery);

        CountRequest countRequest = new CountRequest("dataset");
        countRequest.source(searchSourceBuilder);

        client.countAsync(countRequest, RequestOptions.DEFAULT, new ActionListener<CountResponse>() {
            @Override
            public void onResponse(CountResponse countResponse) {
                handler.handle(Future.succeededFuture(countResponse.getCount()));
            }

            @Override
            public void onFailure(Exception e) {
                handler.handle(Future.failedFuture(e));
            }
        });
    }

}
