package io.piveau.hub.search.util.search;

import io.piveau.hub.search.util.request.Field;
import io.piveau.hub.search.util.request.Query;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedTopHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SearchResponseHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SearchResponseHelper.class);

    public static JsonArray processSearchResult(SearchResponse searchResponse,
                                                Query query,
                                                Map<String, ImmutablePair<String, String>> datasetFacets,
                                                JsonArray datasets,
                                                JsonArray countDatasets,
                                                Map<String, Field> fields) {
        JsonArray results = new JsonArray();

        SearchHit[] searchHits = searchResponse.getHits().getHits();

        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            JsonObject hitResult;
            if (fields == null || (query.getIncludes() != null && !query.getIncludes().isEmpty())) {
                hitResult = new JsonObject(sourceAsMap);
            } else {
                hitResult = new JsonObject();
                for (String field : fields.keySet()) {
                    if (hit.getIndex().equals("dataset") && field.equals("distributions")
                            && sourceAsMap.get("distributions") == null) {
                        hitResult.put("distributions", new JsonArray());
                    } else {
                        hitResult.put(field, sourceAsMap.get(field));
                    }
                }
            }

            DocumentField doc = hit.getFields().get("_ignored");

            if (doc != null) {
                for (Object value : doc.getValues()) {
                    hitResult.putNull(value.toString());
                }
            }

            if (query.isElasticId()) {
                hitResult.put("_id", hit.getId());
            }

            if (hit.getIndex().equals("catalogue")) {
                countDatasets.add(hitResult);
            }

            if (hit.getIndex().equals("dataset")) {
                datasets.add(hitResult);
                if (query.isFilterDistributions()) {
                    for (String facet : datasetFacets.keySet()) {
                        String[] facetSplit = facet.split("\\.");
                        if (facetSplit.length == 2 && facetSplit[0].equals("distributions")) {
                            String facetSplitDist = facetSplit[1];
                            if (query.getFacets().get(facetSplitDist) != null) {
                                JsonArray distributions = hitResult.getJsonArray("distributions");
                                JsonArray distributionsFiltered = new JsonArray();

                                for (Object distribution : distributions) {
                                    JsonObject distJson = (JsonObject) distribution;
                                    JsonObject distFacet = distJson.getJsonObject(facetSplitDist);
                                    if (distFacet != null) {
                                        if (Arrays.asList(query.getFacets().get(facetSplitDist))
                                                .contains(distFacet.getString("id"))) {
                                            distributionsFiltered.add(distJson);
                                        }
                                    }
                                }
                                hitResult.put("distributions", distributionsFiltered);
                            }
                        }
                    }
                }
            }

            results.add(hitResult);
        }

        return results;
    }

    public static JsonArray processAggregationResult(Query query,
                                                     SearchResponse aggregationResponse,
                                                     List<String> facetOrder) {
        if (aggregationResponse != null && aggregationResponse.getAggregations() != null) {
            JsonObject facets = new JsonObject();

            Global globalAggregation = aggregationResponse.getAggregations().get("global");

            Aggregations aggregations;
            if (globalAggregation != null) {
                aggregations = globalAggregation.getAggregations();
            } else {
                aggregations = aggregationResponse.getAggregations();
            }

            for (Aggregation agg : aggregations) {
                JsonObject facet = new JsonObject()
                        .put("id", agg.getName())
                        .put("title", agg.getMetaData().get("title"))
                        .put("items", new JsonArray());

                int count = 0;

                if (agg.getClass().getName().equals(ParsedStringTerms.class.getName())) {
                    for (Terms.Bucket bucket : ((ParsedStringTerms) agg).getBuckets()) {

                        ParsedTopHits topHits = bucket.getAggregations().get("topHits");

                        String id = bucket.getKey().toString().toLowerCase();

                        JsonObject item = new JsonObject()
                                .put("count", bucket.getDocCount())
                                .put("id", id);

                        for (SearchHit hit : topHits.getHits()) {
                            JsonObject dataset = new JsonObject(hit.getSourceAsMap());

                            String title;
                            if (agg.getName().equals("catalog")) {
                                // title = getCatalogTitle(dataset, id);
                                title = id;
                            } else {
                                title = getFacetTitle(dataset, id);
                            }

                            if (!title.isEmpty()) {
                                item.put("title", title);
                                break;
                            }
                        }

                        if ((query.getAggregationLimit() <= 0 || count < query.getAggregationLimit())
                                && bucket.getDocCount() >= query.getAggregationMinCount()) {
                            facet.getJsonArray("items").add(item);
                            count++;
                        }
                    }
                }
                facets.put(facet.getString("id"), facet);
            }

            JsonArray facetsOrdered = new JsonArray();
            LOG.debug(facetOrder.toString());
            for (String f : facetOrder) {
                if (facets.getJsonObject(f) != null) {
                    facetsOrdered.add(facets.getJsonObject(f));
                }
            }
            return facetsOrdered;
        }

        return null;
    }

    private static JsonArray getFacetArray(JsonObject dataset) {
        JsonArray result = new JsonArray();
        for (String key : dataset.getMap().keySet()) {
            Object value = dataset.getValue(key);

            if (value instanceof JsonArray) {
                for (Object arrayValue : (JsonArray) value) {
                    if (arrayValue instanceof JsonObject) {
                        JsonObject arrayValueJson = (JsonObject) arrayValue;
                        if (arrayValueJson.getMap().containsKey("id")
                                && arrayValueJson.getMap().containsKey("title")) {
                            result.add(arrayValueJson);
                        } else {
                            result.addAll(getFacetArray(arrayValueJson));
                        }
                    }
                }
            }

            if (value instanceof JsonObject) {
                result.add(value);
            }
        }
        return result;
    }

    private static String getFacetTitle(JsonObject dataset, String id) {
        JsonArray values = getFacetArray(dataset);
        for (Object value : values) {
            JsonObject current = (JsonObject) value;
            String currentId = current.getString("id");
            if (currentId != null && currentId.toLowerCase().equals(id)) {
                String title = current.getString("title");
                if (title != null) {
                    return title;
                }
            }
        }
        return id;
    }

    public static String getCatalogTitle(JsonObject dataset, String id) {
        JsonObject catalog = dataset.getJsonObject("catalog");
        JsonArray catalog_languages = catalog.getJsonArray("languages");

        String catalog_language = "";
        if (catalog_languages != null && !catalog_languages.isEmpty()) {
            catalog_language = catalog_languages.getString(0);
        }

        String title = null;
        if (catalog_language != null && !catalog_language.isEmpty()) {
            JsonObject catalog_title = catalog.getJsonObject("title");
            if (catalog_title != null) {
                title = catalog_title.getString(catalog_language.toLowerCase());
            }
        }

        if (title == null) {
            JsonObject catalog_title = catalog.getJsonObject("title");
            if (catalog_title != null) {
                title = catalog_title.getString("en");
            }
        }

        if (title == null) {
            title = id;
        }

        return title;
    }

}
