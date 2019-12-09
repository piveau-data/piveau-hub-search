package io.piveau.hub.search.handler;

import io.piveau.hub.search.services.datasets.DatasetsService;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CkanHandler extends SearchHandler {

    private DatasetsService datasetsService;

    public CkanHandler(Vertx vertx, String searchAddress, String datasetsAddress) {
        super(vertx, searchAddress);
        datasetsService = DatasetsService.createProxy(vertx, datasetsAddress);
    }

    private JsonObject facetEsToCkan = new JsonObject()
            .put("keywords", "tags")
            .put("licence", "license_id")
            .put("catalog", "organization")
            .put("categories", "groups")
            .put("format", "res_format")
            .put("country", "country");

    private JsonObject facetCkanToEs = new JsonObject()
            .put("tags", "keywords")
            .put("license_id", "licence")
            .put("organization", "catalog")
            .put("groups", "categories")
            .put("res_format", "format")
            .put("country", "country");

    private static final Logger LOG = LoggerFactory.getLogger(CkanHandler.class);

    public void package_search(RoutingContext context) {
        MultiMap params = context.request().params();

        JsonObject query = new JsonObject();

        String q = params.get("q");
        params.remove("q");
        if(q != null)
            query.put("q", q);

        String fq = params.get("fq");
        params.remove("fq");

        if(fq != null) {
            String[] fqs = fq.split("\\+");

            query.put("facets", new JsonObject());

            for(String facetQuery : fqs) {
                String[] facetQueryArray = facetQuery.split(":");
                if(facetQueryArray.length == 2) {
                    facetQueryArray[0] = facetCkanToEs.getString(facetQueryArray[0]);
                    JsonArray facetQueryJsonArray = query.getJsonObject("facets")
                            .getJsonArray(facetQueryArray[0], new JsonArray()).add(facetQueryArray[1]);
                    query.getJsonObject("facets").put(facetQueryArray[0], facetQueryJsonArray);
                }
            }
        }

        String start = params.get("start");
        params.remove("start");
        if(start != null)
            query.put("from", start);

        String rows = params.get("rows");
        params.remove("rows");
        if(rows != null)
            query.put("size", rows);

        String facet = params.get("facet");
        params.remove("facet");
        if(facet != null)
            query.put("aggregation", facet);

        String sort = params.get("sort");
        params.remove("sort");
        if (sort == null || sort.isEmpty()) {
            sort = "metadata_modified desc";
        }

        JsonArray array;

        String[] sortArray = sort.split(",");
        List<String> sortList = new ArrayList<>();
        for(String s : sortArray) {
            String s_new = s.replaceAll("metadata_modified", "modification_date");
            s_new = s_new.replaceAll("name", "idName"); // TODO: title, depends on language?
            s_new = s_new.replaceAll("relevance", "relevance");
            if(s_new.charAt(0) == ' ') {
                s_new = s_new.substring(1);
            }
            s_new = s_new.replaceAll(" ", "+");
            sortList.add("\"" + s_new + "\"");
        }

        try {
            array = new JsonArray("[" + String.join(",", sortList) + "]");
        } catch (DecodeException e) {
            array = new JsonArray();
        }

        query.put("sort", array);

        final String sort_ckan = sort;

        String facet_mincount = params.get("facet.mincount");
        params.remove("facet_mincount");
        if(facet_mincount != null)
            query.put("aggregationMinCount", facet_mincount);

        String facet_limit = params.get("facet.limit");
        params.remove("facet_limit");
        if(facet_limit != null)
            query.put("aggregationLimit", facet_limit);


        String facet_field = params.get("facet.field");
        params.remove("facet_field");
        JsonArray aggregationFieldsCkan = null;
        try {
            if(facet_field != null) {
                aggregationFieldsCkan = new JsonArray(facet_field);
                JsonArray aggregationFieldsEs = new JsonArray();
                aggregationFieldsCkan.forEach(value -> {
                    if (facetCkanToEs.getString(value.toString()) != null)
                        aggregationFieldsEs.add(facetCkanToEs.getString(value.toString()));
                });
                query.put("aggregationFields", aggregationFieldsEs);
            }
        } catch (DecodeException e) {
            LOG.debug("facet.field DecodeException");
        }
        final JsonArray aggregationFields = aggregationFieldsCkan;

        query.put("aggregationAllFields", false);
        query.put("filter", "dataset");

        LOG.debug(query.toString());

        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().putHeader("Access-Control-Allow-Origin", "*");
        searchService.search(query.toString(), ar -> {
            if(ar.succeeded()) {
                response.put("success", true);

                JsonObject result = ar.result().getJsonObject("result");

                JsonObject result_ckan = new JsonObject();

                JsonArray results = result.getJsonArray("results");
                JsonArray facets = result.getJsonArray("facets");

                JsonArray results_ckan = new JsonArray();
                JsonObject facets_ckan = new JsonObject();
                JsonObject search_facets_ckan = new JsonObject();

                results.forEach(value -> {
                    JsonObject valueJson = new JsonObject(value.toString());
                    results_ckan.add(translateResultToCkan(valueJson));
                });

                if(aggregationFields != null) {
                    aggregationFields.forEach(value -> {
                        facets_ckan.put(value.toString(), new JsonObject());
                        search_facets_ckan.put(value.toString(), new JsonObject()
                                .put("items", new JsonArray())
                                .put("title", value.toString())
                        );
                    });
                }

                facets.forEach(value -> {
                    JsonObject valueJson = new JsonObject(value.toString());
                    String id = facetEsToCkan.getString(valueJson.getString("id"));
                    facets_ckan.put(id, new JsonObject());
                    valueJson.getJsonArray("items").forEach(item -> {
                        JsonObject itemJson = new JsonObject(item.toString());
                        facets_ckan.getJsonObject(id).put(itemJson.getString("id"), itemJson.getInteger("count"));
                    });
                    search_facets_ckan.put(id, translateFacetToCkan(valueJson.getJsonArray("items")));
                });

                result_ckan.put("count", result.getInteger("count"));
                result_ckan.put("sort", sort_ckan);
                result_ckan.put("facets", facets_ckan);
                result_ckan.put("results", results_ckan);
                result_ckan.put("search_facets", search_facets_ckan);

                response.put("result", result_ckan);

                context.response().setStatusCode(200);
            } else {
                response.put("success", false);
                JsonObject result = new JsonObject(ar.cause().getMessage());
                response.put("result", result);
                Integer status = (Integer) result.remove("status");
                context.response().setStatusCode(status);
            }
            context.response().end(response.toString());
        });
    }

    public void package_show(RoutingContext context) {
        String id = context.request().getParam("id");

        JsonObject response = new JsonObject();
        context.response().putHeader("Content-Type", "application/json");
        context.response().putHeader("Access-Control-Allow-Origin", "*");
        datasetsService.readDataset(id, ar -> {
            if(ar.succeeded()) {
                response.put("success", true);
                JsonObject result = ar.result().getJsonObject("result");
                response.put("result", translateResultToCkan(result));
                context.response().setStatusCode(200);
            } else {
                response.put("success", false);
                JsonObject result = new JsonObject(ar.cause().getMessage());
                response.put("result", result);
                Integer status = (Integer) result.remove("status");
                context.response().setStatusCode(status);
            }
            context.response().end(response.toString());
        });
    }

    private JsonArray translateFacetToCkan(JsonArray input) {
        JsonArray output = new JsonArray();

        input.forEach(value -> {
            JsonObject valueJson = new JsonObject(value.toString());
            JsonObject outJson = new JsonObject();

            outJson.put("count", valueJson.getInteger("count"));
            outJson.put("display-name", valueJson.getString("title"));
            outJson.put("name", valueJson.getString("id"));

            output.add(outJson);
        });

        return output;
    }

    private JsonObject translateResultToCkan(JsonObject input) {
        JsonObject output = new JsonObject();

        output.put("id", input.getString("id"));
        output.put("name", input.getString("idName")); // TODO change idName to name
        output.put("type", "dataset");
        output.put("metadata_created", input.getString("release_date"));
        output.put("metadata_modified", input.getString("modification_date"));
        output.put("contact_points", input.getJsonArray("contact_points"));
        output.put("publisher", input.getJsonObject("publisher"));
        output.put("organization", input.getJsonObject("catalog"));
        output.put("identifiers", input.getJsonArray("identifiers"));
        output.put("languages", input.getJsonArray("languages"));
        output.put("version", input.getString("version"));

        JsonArray keywords = input.getJsonArray("keywords");
        JsonArray tags = new JsonArray();
        if(keywords != null) {
            keywords.forEach(value -> {
                JsonObject keyword = (JsonObject) value;
                JsonObject tag = new JsonObject()
                        .put("display-name", keyword.getString("title"))
                        .put("name", keyword.getString("title"))
                        .put("id", keyword.getString("id"));
                tags.add(tag);
            });
        }
        output.put("tags", tags);

        JsonArray temporal_coverages = input.getJsonArray("temporal_coverages");
        JsonArray temporal = new JsonArray();
         if(temporal_coverages != null) {
            temporal_coverages.forEach(value -> {
                JsonObject tempc = (JsonObject) value;
                JsonObject temp = new JsonObject()
                        .put("start_date", tempc.getString("gte"))
                        .put("end_date", tempc.getString("lte"));
                temporal.add(temp);
            });
        }
        output.put("temporal", temporal);

        JsonArray categories = input.getJsonArray("categories");
        JsonArray groups = new JsonArray();
        if(categories != null) {
            categories.forEach(value -> {
                JsonObject category = (JsonObject) value;
                JsonObject group = new JsonObject()
                        .put("display-name", category.getString("title"))
                        .put("name", category.getString("title"))
                        .put("id", category.getString("id"));
                groups.add(group);
            });
        }
        output.put("groups", groups);

        JsonObject country = input.getJsonObject("country");
        if(country != null) {
            String country_id = country.getString("id");

            output.put("translation", new JsonObject());

            if(input.getJsonObject("title") != null) {
                input.getJsonObject("title").forEach(pair -> {
                    if(pair.getKey().equalsIgnoreCase(country_id)) {
                        output.put("title", pair.getValue());
                    } else {
                        if (output.getJsonObject("translation").getJsonObject(pair.getKey()) == null) {
                            output.getJsonObject("translation").put(pair.getKey(), new JsonObject().put("title", pair.getValue()));
                        } else {
                            output.getJsonObject("translation").getJsonObject(pair.getKey()).put("title", pair.getValue());
                        }
                    }
                });
            }

            if(input.getJsonObject("description") != null) {
                input.getJsonObject("description").forEach(pair -> {
                    if (pair.getKey().equalsIgnoreCase(country_id)) {
                        output.put("notes", pair.getValue());
                    } else {
                        if (output.getJsonObject("translation").getJsonObject(pair.getKey()) == null) {
                            output.getJsonObject("translation").put(pair.getKey(), new JsonObject().put("notes", pair.getValue()));
                        } else {
                            output.getJsonObject("translation").getJsonObject(pair.getKey()).put("notes", pair.getValue());
                        }
                    }
                });
            }
        }

        JsonArray distributions = input.getJsonArray("distributions");
        JsonArray resources = new JsonArray();
        if(distributions != null && country != null) {
            String country_id = country.getString("id");
            output.put("num_resources", distributions.size());
            distributions.forEach(value -> {
                JsonObject dist = (JsonObject) value;
                JsonObject licence = dist.getJsonObject("licence");
                if (licence != null) {
                    output.put("licence_id", licence.getString("id"));
                } else {
                    output.putNull("licence_id");
                }
                JsonObject res = new JsonObject();
                res.put("id", dist.getString("id"));
                res.put("access_url", dist.getString("access_url"));
                if(dist.getJsonObject("format") != null) {
                    res.put("format", dist.getJsonObject("format").getString("id"));
                } else {
                    res.putNull("format");
                }
                if (licence != null) {
                    res.put("licence", new JsonObject().put("resource", licence.getString("resource")));
                } else {
                    output.putNull("licence");
                }
                res.put("translation", new JsonObject());
                res.put("size", dist.getInteger("byte_size"));
                res.put("created", dist.getString("release_date"));
                res.put("last_modified", dist.getString("modification_date"));
                res.put("state", dist.getString("status"));

                if(dist.getJsonObject("title") != null) {
                    dist.getJsonObject("title").forEach(pair -> {
                        if(pair.getKey().equalsIgnoreCase(country_id)) {
                            res.put("title", pair.getValue());
                        } else {
                            if (res.getJsonObject("translation").getJsonObject(pair.getKey()) == null) {
                                res.getJsonObject("translation").put(pair.getKey(), new JsonObject().put("title", pair.getValue()));
                            } else {
                                res.getJsonObject("translation").getJsonObject(pair.getKey()).put("title", pair.getValue());
                            }
                        }
                    });
                }

                if(dist.getJsonObject("description") != null) {
                    dist.getJsonObject("description").forEach(pair -> {
                        if (pair.getKey().equalsIgnoreCase(country_id)) {
                            res.put("notes", pair.getValue());
                        } else {
                            if (res.getJsonObject("translation").getJsonObject(pair.getKey()) == null) {
                                res.getJsonObject("translation").put(pair.getKey(), new JsonObject().put("notes", pair.getValue()));
                            } else {
                                res.getJsonObject("translation").getJsonObject(pair.getKey()).put("notes", pair.getValue());
                            }
                        }
                    });
                }

                resources.add(res);
            });
        }
        output.put("resources", resources);

        return output;
    }
}
