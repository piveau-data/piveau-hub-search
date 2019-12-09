package io.piveau.hub.search.util.index;

import io.piveau.hub.search.util.request.Field;
import io.piveau.utils.ConfigHelper;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class IndexManager {

    private static final Logger LOG = LoggerFactory.getLogger(IndexManager.class);

    // index -> fields
    private final Map<String, Map<String, Field>> fields = new HashMap<>();

    // index -> facets
    private final Map<String, Map<String, ImmutablePair<String, String>>> facets = new HashMap<>();

    // index -> facetOrder
    private final Map<String, List<String>> facetOrder = new HashMap<>();

    // index -> searchParams
    private final Map<String, Map<String, String>> searchParams = new HashMap<>();

    // index -> maxAggSize ; defines the maximum size of an aggregation
    private final Map<String, Integer> maxAggSize = new HashMap<>();

    // index -> maxResultWindow ; defines the maximum result of from + size
    private final Map<String, Integer> maxResultWindow = new HashMap<>();

    // index -> settings filepath
    private final Map<String, String> settingsFilepath = new HashMap<>();

    // index -> mapping filepath
    private final Map<String, String> mappingFilepath = new HashMap<>();

    // vertx context
    private Vertx vertx;

    // elasticsearch config
    private JsonObject config;

    public static IndexManager create(Vertx vertx, JsonObject config, Handler<AsyncResult<IndexManager>> handler) {
        return new IndexManager(vertx, config, handler);
    }

    private IndexManager(Vertx vertx, JsonObject config, Handler<AsyncResult<IndexManager>> handler) {
        this.vertx = vertx;

        this.config = config;

        JsonObject index = ConfigHelper.forConfig(config).getJson("index");

        JsonObject dataset = ConfigHelper.forConfig(index).getJson("dataset");
        JsonObject catalogue = ConfigHelper.forConfig(index).getJson("catalogue");

        if (dataset.isEmpty() || catalogue.isEmpty()) {
            handler.handle(Future.failedFuture("Index config is missing!"));
        } else {
            List<Future> indexFutureList = new ArrayList<>();

            indexFutureList.add(initIndex("dataset", dataset));
            indexFutureList.add(initIndex("catalogue", catalogue));

            CompositeFuture.all(indexFutureList).setHandler(indexFutureHandler -> {
                if (indexFutureHandler.succeeded()) {
                    handler.handle(Future.succeededFuture(this));
                } else {
                    LOG.error("Init indexes: " + indexFutureHandler.cause());
                    handler.handle(Future.succeededFuture(this));
                }
            });
        }
    }

    private Future initIndex(String index, JsonObject indexJson) {
        Future<Void> indexFuture = Future.future();

        String indexSettingsFilepath = indexJson.getString("settings");
        String indexMappingFilepath = indexJson.getString("mapping");
        JsonArray indexFacets = indexJson.getJsonArray("facets");
        JsonArray indexSearchParams = indexJson.getJsonArray("searchParams");
        Integer indexMaxAggSize = indexJson.getInteger("max_agg_size", 50);
        Integer indexMaxResultWindow = indexJson.getInteger("max_result_window", 10000);

        if (indexSettingsFilepath == null || indexMappingFilepath == null) {
            indexFuture.fail("Index config incorrect!");
        } else {
            facets.putIfAbsent(index, new HashMap<>());
            List<String> facetOrderList = new ArrayList<>();
            if (indexFacets != null) {
                for (Object facet : indexFacets) {
                    JsonObject facetJson = (JsonObject) facet;
                    String facetName = facetJson.getString("name");
                    String facetTitle = facetJson.getString("title");
                    String facetPath = facetJson.getString("path");
                    if (facetName == null || facetPath == null) {
                        indexFuture.fail("Index config incorrect!");
                        break;
                    }
                    facets.get(index).putIfAbsent(facetName, new ImmutablePair<>(facetTitle, facetPath));
                    facetOrderList.add(facetName);
                }
            }
            facetOrder.putIfAbsent(index, facetOrderList);

            searchParams.putIfAbsent(index, new HashMap<>());
            if (indexSearchParams != null) {
                for (Object searchParam : indexSearchParams) {
                    JsonObject searchParamJson = (JsonObject) searchParam;
                    String searchParamName = searchParamJson.getString("name");
                    String searchParamField = searchParamJson.getString("field");
                    if (searchParamName == null || searchParamField == null) {
                        indexFuture.fail("Index config incorrect!");
                        break;
                    }
                    searchParams.get(index).putIfAbsent(searchParamName, searchParamField);
                }
            }

            maxAggSize.putIfAbsent(index, indexMaxAggSize);
            maxResultWindow.putIfAbsent(index, indexMaxResultWindow);

            settingsFilepath.putIfAbsent(index, indexSettingsFilepath);
            mappingFilepath.putIfAbsent(index, indexMappingFilepath);

            parseMapping(index, indexMappingFilepath);
            indexFuture.complete();
        }

        return indexFuture;
    }

    private void parseMapping(String index, String indexMappingFilePath) {
        fields.putIfAbsent(index, new HashMap<>());
        vertx.fileSystem().readFile(indexMappingFilePath, ar -> {
            if (ar.succeeded()) {
                parseMapping(
                        new JsonObject(ar.result().toString()).getJsonObject("properties"),
                        ConfigHelper.forConfig(config).getJson("boost"),
                        fields.get(index),
                        null
                );
            }
        });
    }

    private void parseMapping(JsonObject mapping, JsonObject boost, Map<String, Field> fields, Field parent) {
        for (Map.Entry<String, Object> entry : mapping) {
            JsonObject fieldJson = (JsonObject) entry.getValue();
            Field field = new Field(entry.getKey(), ((JsonObject) entry.getValue()).getString("type"));
            if (boost != null)
                field.setBoost(boost.getFloat(field.getName(), 1.0f));
            JsonObject properties = fieldJson.getJsonObject("properties");

            if (properties != null) {
                field.setSubFields(new ArrayList<>());
                parseMapping(properties, boost, fields, field);
            } else {
                Boolean enabled = fieldJson.getBoolean("enabled");
                String type = fieldJson.getString("type");
                if ((enabled == null || enabled) && (type.equals("text") || type.equals("keyword"))) {
                    field.setSearchable(true);
                    if (parent != null) {
                        parent.setSearchable(true);
                    }
                }
            }

            if (parent == null) {
                fields.put(field.getName(), field);
            } else {
                parent.getSubFields().add(field);
            }
        }
    }

    public void prepareIndexCreateRequest(String index, Handler<AsyncResult<CreateIndexRequest>> handler) {
        vertx.fileSystem().readFile(settingsFilepath.get(index), ar -> {
            if (ar.succeeded()) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
                createIndexRequest.settings(
                        Settings.builder().loadFromSource(ar.result().toString(), XContentType.JSON)
                );
                handler.handle(Future.succeededFuture(createIndexRequest));
            } else {
                LOG.error("Failed to read settings file: " + ar.cause());
                handler.handle(Future.failedFuture("Failed to read settings file: " + ar.cause().getMessage()));
            }
        });
    }

    public void prepareIndexDeleteRequest(String index, Handler<AsyncResult<DeleteIndexRequest>> handler) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        handler.handle(Future.succeededFuture(deleteIndexRequest));
    }

    public void preparePutMappingRequest(String index, Handler<AsyncResult<PutMappingRequest>> handler) {
        vertx.fileSystem().readFile(mappingFilepath.get(index), ar -> {
            if (ar.succeeded()) {
                PutMappingRequest putMappingRequest = new PutMappingRequest(index);
                putMappingRequest.source(ar.result().toString(), XContentType.JSON);
                handler.handle(Future.succeededFuture(putMappingRequest));
            } else {
                handler.handle(Future.failedFuture("Failed to read mappings file: " + ar.cause().getMessage()));
            }
        });
    }

    public void boost(String index, String field, Float value, Handler<AsyncResult<String>> handler) {
        String[] keys = field.split("\\.");

        if (fields.get(index) == null) {
            handler.handle(Future.failedFuture("Index doesn't exists"));
        } else if (keys.length == 0) {
            handler.handle(Future.failedFuture("No field provided"));
        } else {
            Field current = fields.get(index).get(keys[0]);

            for (int i = 1; i < keys.length; ++i) {
                if (current.getSubFields() != null) {
                    for (Field subField : current.getSubFields()) {
                        if (subField.getName().equals(keys[i])) {
                            current = subField;
                        }
                    }
                } else {
                    handler.handle(Future.failedFuture("Wrong field name"));
                    return;
                }
            }

            current.setBoost(value);
            handler.handle(Future.succeededFuture(current.toString()));
        }
    }

    public void prepareSetMaxResultWindowRequest(String index, Integer maxResultWindow,
                                                 Handler<AsyncResult<UpdateSettingsRequest>> handler) {
        if (maxResultWindow == null) {
            handler.handle(Future.failedFuture("Max result window missing"));
        } else if (maxResultWindow < 10000) {
            handler.handle(Future.failedFuture("Max result window has to be greate than 10000"));
        } else {
            Settings settings = Settings.builder().put("index.max_result_window", maxResultWindow).build();
            UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index).settings(settings);
            handler.handle(Future.succeededFuture(updateSettingsRequest));
        }
    }

    public void setMaxAggSize(String index, Integer maxAggSize, Handler<AsyncResult<String>> handler) {
        if (maxAggSize == null) {
            handler.handle(Future.failedFuture("Max aggregation size missing"));
        } else if (maxAggSize <= 0) {
            handler.handle(Future.failedFuture("Max aggregation size has to be greater zero"));
        } else {
            this.maxAggSize.put(index, maxAggSize);
            handler.handle(Future.succeededFuture("Successfully set max_agg_size = " + this.maxAggSize
                    + " for index ( " + index + ")"));
        }
    }

    public void setMaxResultWindow(String index, Integer maxResultWindow) {
        this.maxResultWindow.put(index, maxResultWindow);
    }

    public Map<String, Map<String, Field>> getFields() {
        return fields;
    }

    public Map<String, Map<String, ImmutablePair<String, String>>> getFacets() {
        return facets;
    }

    public Map<String, List<String>> getFacetOrder() {
        return facetOrder;
    }

    public Map<String, Map<String, String>> getSearchParams() {
        return searchParams;
    }

    public Map<String, Integer> getMaxAggSize() {
        return maxAggSize;
    }

    public Map<String, Integer> getMaxResultWindow() {
        return maxResultWindow;
    }
}
