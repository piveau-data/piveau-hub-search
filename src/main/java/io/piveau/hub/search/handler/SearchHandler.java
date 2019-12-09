package io.piveau.hub.search.handler;

import io.piveau.hub.search.services.search.SearchService;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SearchHandler extends ContextHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SearchHandler.class);

    SearchService searchService;

    public SearchHandler(Vertx vertx, String address) {
        searchService = SearchService.createProxy(vertx, address);
    }

    public void searchPost(RoutingContext context) {
        searchService.search(context.getBodyAsString(), ar -> handleContext(context, ar));
    }

    JsonObject paramsToQuery(MultiMap params) {
        JsonObject query = new JsonObject();

        String q = params.get("q");
        params.remove("q");
        if(q != null)
            query.put("q", q);

        Integer page;
        try {
            page = Integer.parseInt(params.get("page"));
        } catch (NumberFormatException e) {
            page = 0;
        }
        params.remove("page");

        Integer limit;
        try {
            limit = Integer.parseInt(params.get("limit"));
        } catch (NumberFormatException e) {
            limit = 10;
        }
        params.remove("limit");

        query.put("from", page*limit);
        query.put("size", limit);

        String aggregation = params.get("aggregation");
        params.remove("aggregation");
        if(aggregation != null)
            query.put("aggregation", aggregation);

        String onlyIds = params.get("onlyIds");
        params.remove("onlyIds");
        if(onlyIds != null)
            query.put("onlyIds", onlyIds);

        String globalAggregation = params.get("globalAggregation");
        params.remove("globalAggregation");
        if(globalAggregation != null)
            query.put("globalAggregation", globalAggregation);

        String facetOperator = params.get("facetOperator");
        params.remove("facetOperator");
        if(facetOperator != null)
            query.put("facetOperator", facetOperator);

        String facetGroupOperator = params.get("facetGroupOperator");
        params.remove("facetGroupOperator");
        if(facetGroupOperator != null)
            query.put("facetGroupOperator", facetGroupOperator);

        String filterDistributions = params.get("filterDistributions");
        params.remove("filterDistributions");
        if(filterDistributions != null)
            query.put("filterDistributions", filterDistributions);

        List<String> sort = params.getAll("sort");
        params.remove("sort");
        if(!sort.isEmpty()) {
            sort = checkCommaDelimited(sort);
            for (int i = 0; i < sort.size(); i++) {
                sort.set(i, sort.get(i).replaceAll(" ", "+"));
            }
            query.put("sort", new JsonArray(sort));
        }

        String filter = params.get("filter");
        params.remove("filter");
        if(filter != null) {
            query.put("filter", filter);

            JsonObject facetsJson;

            String facets = params.get("facets");
            params.remove("facets");
            try {
                facetsJson = new JsonObject(facets);
            } catch (DecodeException e) {
                facetsJson = new JsonObject();
            }

            if(!facetsJson.isEmpty())
                query.put("facets", facetsJson);
        }

        List<String> fields = params.getAll("fields");
        params.remove("fields");
        if(!fields.isEmpty())
            query.put("fields", new JsonArray(checkCommaDelimited(fields)));

        List<String> includes = params.getAll("includes");
        params.remove("includes");
        if(!includes.isEmpty())
            query.put("includes", new JsonArray(checkCommaDelimited(includes)));

        String minDate = params.get("minDate");
        params.remove("minDate");
        String maxDate = params.get("maxDate");
        params.remove("maxDate");
        String bboxMinLon = params.get("bboxMinLon");
        params.remove("bboxMinLon");
        String bboxMaxLon = params.get("bboxMaxLon");
        params.remove("bboxMaxLon");
        String bboxMaxLat = params.get("bboxMaxLat");
        params.remove("bboxMaxLat");
        String bboxMinLat = params.get("bboxMinLat");
        params.remove("bboxMinLat");

        if(filter != null && !filter.isEmpty() && !filter.equals("autocomplete")) {
            JsonObject searchParams = new JsonObject();
            if(minDate != null) {
                searchParams.put("minDate", minDate);
            }
            if(maxDate != null) {
                searchParams.put("maxDate", maxDate);
            }
            if(bboxMinLon != null && bboxMaxLon != null && bboxMaxLat != null && bboxMinLat != null) {
                JsonObject boundingBox = new JsonObject();
                boundingBox.put("minLon", bboxMinLon);
                boundingBox.put("maxLon", bboxMaxLon);
                boundingBox.put("maxLat", bboxMaxLat);
                boundingBox.put("minLat", bboxMinLat);
                searchParams.put("boundingBox", boundingBox);
            }
            if(!searchParams.isEmpty())
                query.put("searchParams", searchParams);
        }

        for(Map.Entry<String, String> entry : params.entries()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.contains(".")) {
                String[] splitKey = key.split("\\.");
                JsonObject keyObject = query.getJsonObject(splitKey[0], new JsonObject());

                if (splitKey[0].equals("boost")) {
                    try {
                        String boostField = "";
                        for(int i = 1; i < splitKey.length; ++i) {
                            boostField = boostField.concat(splitKey[i]);
                            if (i != splitKey.length-1) {
                                boostField = boostField.concat(".");
                            }
                        }
                        keyObject.put(boostField, Float.valueOf(value));
                    } catch (NumberFormatException e) {
                        continue;
                    }
                } else {
                    keyObject.put(splitKey[1], value);
                }

                query.put(splitKey[0], keyObject);
            }
        }

        return query;
    }

    public void searchGet(RoutingContext context) {
        MultiMap params = context.request().params();

        LOG.debug(params.toString());

        JsonObject query = paramsToQuery(params);

        LOG.debug(query.encodePrettily());

        searchService.search(query.toString(), ar -> handleContext(context, ar));
    }

    public void searchAutocomplete(RoutingContext context) {
        JsonObject query = new JsonObject();
        MultiMap params = context.request().params();
        String q = params.get("q");
        if(q != null)
            query.put("q", q);
        query.put("filter", "autocomplete");

        searchService.search(query.toString(), ar -> handleContext(context, ar));
    }

    private List<String> checkCommaDelimited(List<String> input) {
        List<String> output;

        if(input.size() == 1)
            output =  Arrays.asList(input.get(0).split(","));
        else
            output = input;

        for (int i = 0; i < output.size(); i++) {
            output.set(i, output.get(i).trim());
        }

        return output;
    }
}
