package io.piveau.hub.search.util.search;

import io.piveau.hub.search.models.Constants;
import io.piveau.hub.search.util.request.Field;
import io.piveau.hub.search.util.request.Query;
import io.piveau.hub.search.util.geo.BoundingBox;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;

public class BuildQueryHelper {

    private static final Logger LOG = LoggerFactory.getLogger(BuildQueryHelper.class);

    static BoolQueryBuilder buildQuery(Query query) {
        BoolQueryBuilder fullQuery = QueryBuilders.boolQuery();
        QueryBuilder qQuery = buildQQuery(query, null);
        fullQuery.must(qQuery);
        return fullQuery;
    }

    static BoolQueryBuilder buildQuery(
            Query query,
            Map<String, Field> fields,
            Map<String, ImmutablePair<String, String>> facets,
            Map<String, String> searchParams) {

        BoolQueryBuilder fullQuery = QueryBuilders.boolQuery();

        QueryBuilder qQuery = buildQQuery(query, fields);

        fullQuery.must(qQuery);

        if (query.getFilter() != null && !query.getFilter().isEmpty() && !query.getFilter().equals("autocomplete")) {
            if (query.getFacets() != null) {
                BoolQueryBuilder facetQuery = buildFacetQuery(
                        query,
                        facets
                );

                fullQuery.must(facetQuery);
            }

            if (query.getSearchParams() != null) {
                RangeQueryBuilder dateQuery = buildDateQuery(
                        query.getSearchParams().getMinDate(),
                        query.getSearchParams().getMaxDate(),
                        searchParams.get("temporal")
                );

                if (dateQuery != null) fullQuery.must(dateQuery);

                GeoShapeQueryBuilder spatialQuery = buildSpatialQuery(
                        query.getSearchParams().getBoundingBox(),
                        searchParams.get("spatial")
                );

                if (spatialQuery != null) fullQuery.filter(spatialQuery);
            }
        }

        return fullQuery;
    }

    private static QueryBuilder buildQQuery(Query query, Map<String, Field> fields) {
        QueryBuilder qQuery;

        if (query.getQ() == null || query.getQ().isEmpty()) {
            qQuery = QueryBuilders.matchAllQuery();
        } else {
            Map<String, Float> multiMatchFields = determineMultiMatchFields(query, fields);
            qQuery = parseQueryString(query.getQ(), multiMatchFields);
        }

        return qQuery;
    }

    private static BoolQueryBuilder buildFacetQuery(Query query, Map<String, ImmutablePair<String, String>> facets) {
        BoolQueryBuilder facetQuery = QueryBuilders.boolQuery();

        for (String facetName : facets.keySet()) {
            String facetPath = facets.get(facetName).getValue();

            if (query.getFacets().get(facetName) != null && query.getFacets().get(facetName).length != 0) {
                BoolQueryBuilder subQuery = QueryBuilders.boolQuery();
                if (query.getFacetOperator().equals(Constants.Operator.AND)) {
                    for (String facet : query.getFacets().get(facetName)) {
                        subQuery.must(QueryBuilders
                                .termQuery(facetPath + ".id", facet));
                    }
                } else {
                    for (String facet : query.getFacets().get(facetName)) {
                        subQuery.should(QueryBuilders
                                .termQuery(facetPath + ".id", facet));
                    }

                    subQuery.minimumShouldMatch(1);
                }
                if (query.getFacetGroupOperator().equals(Constants.Operator.AND)) {
                    facetQuery.must(subQuery);
                } else {
                    facetQuery.should(subQuery);
                }
            }
        }

        if (query.getFacetGroupOperator().equals(Constants.Operator.OR)) {
            facetQuery.minimumShouldMatch(1);
        }

        return facetQuery;
    }

    private static RangeQueryBuilder buildDateQuery(Date minDate, Date maxDate, String dateField) {
        if (dateField != null && !dateField.isEmpty()) {
            if (minDate != null && maxDate == null) {
                return QueryBuilders.rangeQuery(dateField).gte(minDate);
            }
            if (minDate == null && maxDate != null) {
                return QueryBuilders.rangeQuery(dateField).lte(maxDate);
            }
            if (minDate != null /*&& maxDate != null*/) {
                return QueryBuilders.rangeQuery(dateField).gte(minDate).lte(maxDate);
            }
        }
        return null;
    }

    private static GeoShapeQueryBuilder buildSpatialQuery(BoundingBox boundingBox, String spatialField) {
        if (spatialField != null && !spatialField.isEmpty()) {
            if (boundingBox != null) {
                Float minLon = boundingBox.getMinLon();
                Float maxLon = boundingBox.getMaxLon();
                Float maxLat = boundingBox.getMaxLat();
                Float minLat = boundingBox.getMinLat();

                if (minLon != null && maxLon != null && maxLat != null && minLat != null) {
                    Coordinate topLeft = new Coordinate(minLon, maxLat);
                    Coordinate bottomRight = new Coordinate(maxLon, minLat);

                    try {
                        return geoShapeQuery(
                                spatialField,
                                new EnvelopeBuilder(topLeft, bottomRight)
                        );
                    } catch (IOException e) {
                        // e.printStackTrace();
                        LOG.warn("Exception while building bounding box.");
                    }
                }
            }
        }
        return null;
    }

    private static QueryBuilder parseQueryString(String querystring, Map<String, Float> multiMatchFields) {
        querystring = querystring.trim();

        if (querystring.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }

        if (querystring.charAt(0) == '(') {
            int open_braces = 1;
            int i = 1;

            for (; i < querystring.length(); ++i) {
                if (querystring.charAt(i) == ')') {
                    open_braces--;
                }
                if (querystring.charAt(i) == '(') {
                    open_braces++;
                }
                if (open_braces == 0) {
                    break;
                }
            }

            if (i == querystring.length() - 1) {
                querystring = querystring.substring(1, querystring.length() - 1);
            }
        }

        if (querystring.isEmpty()) return QueryBuilders.matchAllQuery();

        String[] or_split = querystring.split("(?<!\\()OR\\b(?![\\w\\s]*[)])");

        if (or_split.length == 1) {
            String[] and_split = querystring.split("(?<!\\()AND\\b(?![\\w\\s]*[)])");

            if (and_split.length == 1) {
                return buildMultiMatch(querystring, multiMatchFields);
            } else {
                BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                for (String s : and_split) {
                    QueryBuilder qb = parseQueryString(s, multiMatchFields);
                    if (qb != null) {
                        boolQuery.must(qb);
                    }
                }
                return boolQuery;
            }
        } else {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            for (String s : or_split) {
                QueryBuilder qb = parseQueryString(s, multiMatchFields);
                if (qb != null) {
                    boolQuery.should(qb);
                }
            }
            return boolQuery;
        }
    }

    private static QueryBuilder buildMultiMatch(String querystring, Map<String, Float> multiMatchFields) {
        boolean phrase = false;
        if (querystring.startsWith("\"") && querystring.endsWith("\"")) {
            querystring = querystring.substring(1, querystring.length() - 1);
            phrase = true;
        }

        querystring = querystring.trim();

        if (querystring.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }

        MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(querystring).lenient(true);

        if (phrase) {
            multiMatchQuery.type(MultiMatchQueryBuilder.Type.PHRASE);
        } else {
            multiMatchQuery.fuzziness(Fuzziness.AUTO);
            multiMatchQuery.prefixLength(4);
            multiMatchQuery.maxExpansions(10);
        }

        multiMatchQuery.fields(multiMatchFields);

        return multiMatchQuery;
    }

    private static Map<String, Float> determineMultiMatchFields(Query query, Map<String, Field> fields) {
        Map<String, Float> multiMatchFields = new HashMap<>();

        if (query.getFilter() != null && !query.getFilter().isEmpty()) {
            if (query.getFilter().equals("autocomplete")) {
                multiMatchFields.put("title.*.autocomplete", 1.0f);
            } else {
                if (fields != null) {
                    if (query.getFields() == null || query.getFields().isEmpty()) {
                        for (Map.Entry<String, Field> entry : fields.entrySet()) {
                            Field field = entry.getValue();
                            addFieldToMultiMatchFields(multiMatchFields, field, query.getBoost().get(field.getName()));
                        }
                    } else {
                        for (String key : query.getFields()) {
                            Field field = fields.get(key);
                            if (field != null) {
                                addFieldToMultiMatchFields(multiMatchFields, field,
                                        query.getBoost().get(field.getName()));
                            }
                        }
                    }
                }
            }
        }

        return multiMatchFields;
    }

    private static void addFieldToMultiMatchFields(Map<String, Float> multiMatchFields, Field field, Float boost) {
        if (boost == null) boost = field.getBoost();
        if (field != null && field.isSearchable()) {
            if (field.getSubFields() != null) {
                for (Field subField : field.getSubFields()) {
                    if (subField.isSearchable()) {
                        // multiMatchQuery.field(field.getName() + "." + subField.getName(), boost);
                        if (subField.getSubFields() != null) {
                            for (Field subSubField : subField.getSubFields()) {
                                if (subSubField.isSearchable()) {
                                    multiMatchFields.put(field.getName() + "."
                                            + subField.getName() + "." + subSubField.getName(), boost);
                                }
                            }
                        } else {
                            multiMatchFields.put(field.getName() + "." + subField.getName(), boost);
                        }
                    }
                }
            } else {
                multiMatchFields.put(field.getName(), boost);
            }
        }
    }

    static TermsAggregationBuilder genTermsAggregation(String path, String name, String title, Integer maxAggSize) {
        String[] includes = new String[1];
        includes[0] = path;

        return AggregationBuilders
                .terms(name)
                .size(maxAggSize)
                .field(path + ".id")
                .setMetaData(genMetaData(title))
                .subAggregation(
                        AggregationBuilders
                                .topHits("topHits")
                                .size(1)
                                .fetchSource(includes, null)
                );
    }

    private static Map<String, Object> genMetaData(String title) {
        HashMap<String, Object> metaData = new HashMap<>();
        metaData.put("title", title);
        return metaData;
    }

}
