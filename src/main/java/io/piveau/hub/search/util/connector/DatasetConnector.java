package io.piveau.hub.search.util.connector;

import io.piveau.hub.search.util.index.IndexManager;
import io.piveau.hub.search.util.response.GetResponseHelper;
import io.piveau.hub.search.util.response.ReturnHelper;
import io.piveau.hub.search.util.geo.Spatial;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

public class DatasetConnector {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetConnector.class);

    // elasticsearch client
    private RestHighLevelClient client;

    // index manager
    private IndexManager indexManager;

    public static DatasetConnector create(Vertx vertx, JsonObject config, Handler<AsyncResult<DatasetConnector>> handler) {
        return new DatasetConnector(vertx, config, handler);
    }

    private DatasetConnector(Vertx vertx, JsonObject config, Handler<AsyncResult<DatasetConnector>> handler) {
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

    private void processBulkResponse(BulkResponse bulkResponse, JsonArray catalogueBulk) {
        int i = 0;
        for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
            if (i >= bulkResponse.getItems().length)
                break;

            while (catalogueBulk.getJsonObject(i) != null &&
                    catalogueBulk.getJsonObject(i).getBoolean("success") != null) {
                i++;
            }

            JsonObject document = catalogueBulk.getJsonObject(i);
            String failure = bulkItemResponse.getFailureMessage();
            if (failure == null) {
                document.put("success", true);
                document.put("status", bulkItemResponse.status().getStatus());
                document.remove("message");
            } else {
                document.put("success", false);
                document.put("status", bulkItemResponse.status().getStatus());
                document.put("message", bulkItemResponse.getFailureMessage());
            }

            i++;
        }
    }

    public void checkSpatial(JsonArray payload, Handler<AsyncResult> handler) {
        List<Future> futureList = new ArrayList<>();
        for (Object value : payload) {
            Future future = Future.future();
            JsonObject valueJson = (JsonObject) value;
            checkSpatial(valueJson, ar -> future.complete());
            futureList.add(future);
        }
        CompositeFuture.all(futureList).setHandler(ar -> handler.handle(Future.succeededFuture()));
    }

    public void checkSpatial(JsonObject payload, Handler<AsyncResult> handler) {
        if (payload.containsKey("spatial")) {
            JsonObject spatial = payload.getJsonObject("spatial");
            if (spatial != null) {
                payload.put("spatial", Spatial.checkSpatial(payload.getJsonObject("spatial")));
            }
        }
        handler.handle(Future.succeededFuture());
    }

    public void createDataset(JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        if (payload == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is null")));
            return;
        }

        String datasetId = UUID.randomUUID().toString();

        payload.put("id", datasetId);

        JsonObject result = new JsonObject().put("id", datasetId);

        IndexRequest indexRequest = new IndexRequest("dataset").id(datasetId).opType("create")
                .source(payload.encode(), XContentType.JSON);
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                LOG.info("Index dataset: Dataset {} created. {}", datasetId, indexResponse.toString());
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(201, result)));
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Index dataset: " + e);
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

    public void createOrUpdateDataset(String datasetId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        if (datasetId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (datasetId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        if (payload == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is null")));
            return;
        }

        payload.put("id", datasetId);

        JsonObject result = new JsonObject().put("id", datasetId);

        IndexRequest indexRequest = new IndexRequest("dataset").id(datasetId)
                .source(payload.encode(), XContentType.JSON);
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                if (indexResponse.status().getStatus() == 200) {
                    // updated
                    LOG.info("Index dataset: Dataset {} updated. {}", datasetId, indexResponse.toString());
                } else {
                    // created
                    LOG.info("Index dataset: Dataset {} created. {}", datasetId, indexResponse.toString());
                }
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(indexResponse.status().getStatus(), result)));
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Index dataset: " + e);
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

    public void modifyDataset(String datasetId, JsonObject payload, Handler<AsyncResult<JsonObject>> handler) {
        if (datasetId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (datasetId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        if (payload == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is null")));
            return;
        }

        JsonObject result = new JsonObject().put("id", datasetId);

        UpdateRequest updateRequest = new UpdateRequest("dataset", datasetId)
                .doc(payload.toString(), XContentType.JSON);
        client.updateAsync(updateRequest, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                LOG.info("Index dataset: Dataset {} modified. {}", datasetId, updateResponse.toString());
                handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, result)));
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Index dataset: " + e);
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

    public void readDataset(String datasetId, Handler<AsyncResult<JsonObject>> handler) {
        if (datasetId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (datasetId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        GetRequest getRequest = new GetRequest("dataset", datasetId);
        client.getAsync(getRequest, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists()) {
                    handler.handle(Future.succeededFuture(
                            ReturnHelper.returnSuccess(200, GetResponseHelper.getResponseToJson(getResponse,
                                    indexManager.getFields().get("dataset")))));
                } else {
                    LOG.error("Read dataset: Dataset {} not found", datasetId);
                    handler.handle(Future.failedFuture(
                            ReturnHelper.returnFailure(404, "Dataset " + datasetId + " not found")));
                }
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Read dataset: " + e);
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

    public void deleteDataset(String datasetId, Handler<AsyncResult<JsonObject>> handler) {
        if (datasetId == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is null")));
            return;
        }

        if (datasetId.isEmpty()) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(400, "ID is empty")));
            return;
        }

        JsonObject result = new JsonObject().put("id", datasetId);

        DeleteRequest deleteRequest = new DeleteRequest("dataset", datasetId);
        client.deleteAsync(deleteRequest, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                if (deleteResponse.status() == RestStatus.NOT_FOUND) {
                    LOG.error("Delete dataset: Dataset {} not found", datasetId);
                    handler.handle(Future.failedFuture(
                            ReturnHelper.returnFailure(404, "Dataset " + datasetId + " not found")));
                } else {
                    handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, result)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Delete dataset: " + e);
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

    public void datasetBulk(JsonArray payload, CopyOnWriteArrayList<JsonObject> datasetBulk, String operation,
                            Handler<AsyncResult<JsonObject>> handler) {
        if (operation == null || !(operation.equals("create") || operation.equals("replace"))) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Operation missing or wrong.")));
            return;
        }

        if (payload == null) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is null")));
            return;
        }

        if (payload.isEmpty() && (datasetBulk == null || datasetBulk.isEmpty())) {
            handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, "Payload is empty")));
            return;
        }

        List<IndexRequest> indexRequestList = new ArrayList<>();

        for (Object obj : payload) {
            JsonObject dataset = new JsonObject(obj.toString());

            // check id
            String datasetId;
            if (operation.equals("create")) {
                datasetId = UUID.randomUUID().toString();
                dataset.put("id", datasetId);
            } else {
                datasetId = dataset.getString("id");
            }

            if (datasetId == null) {
                datasetBulk.add(new JsonObject()
                        .put("success", false)
                        .put("status", 400)
                        .put("message", "ID is null")
                        .putNull("id")
                );
                continue;
            }

            if (datasetId.isEmpty()) {
                datasetBulk.add(new JsonObject()
                        .put("success", false)
                        .put("status", 400)
                        .put("message", "ID is empty")
                        .put("id", datasetId)
                );
                continue;
            }

            JsonObject doc = new JsonObject()
                    .putNull("success")
                    .putNull("status")
                    .putNull("message")
                    .put("id", datasetId);

            if (operation.equals("create")) {
                indexRequestList.add(
                        new IndexRequest("dataset").id(datasetId)
                                .source(dataset.toString(), XContentType.JSON).opType("create")
                );
            }

            if (operation.equals("replace")) {
                indexRequestList.add(
                        new IndexRequest("dataset").id(datasetId)
                                .source(dataset.toString(), XContentType.JSON)
                );
            }

            datasetBulk.add(doc);
        }

        JsonArray datasetBulkJsonArray = new JsonArray();
        for (JsonObject obj : datasetBulk) {
            datasetBulkJsonArray.add(obj);
        }

        BulkRequest bulkRequest = new BulkRequest();
        for (IndexRequest indexRequest : indexRequestList) {
            bulkRequest.add(indexRequest);
        }

        if (bulkRequest.numberOfActions() != 0) {
            client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    processBulkResponse(bulkResponse, datasetBulkJsonArray);

                    JsonObject result = new JsonObject().put("datasets", datasetBulk);
                    handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, result)));
                }

                @Override
                public void onFailure(Exception e) {
                    LOG.error("Bulk dataset: " + e);
                    if (e.getClass().equals(ElasticsearchStatusException.class)) {
                        ElasticsearchStatusException statusException = (ElasticsearchStatusException) e;
                        handler.handle(Future.failedFuture(ReturnHelper.returnFailure(statusException.status().getStatus(),
                                statusException.getMessage())));
                    } else {
                        handler.handle(Future.failedFuture(ReturnHelper.returnFailure(500, e.getMessage())));
                    }
                }
            });
        } else {
            JsonObject result = new JsonObject().put("datasets", datasetBulk);
            handler.handle(Future.succeededFuture(ReturnHelper.returnSuccess(200, result)));
        }
    }

}
