package io.piveau.hub.search.util.search;

import io.piveau.hub.search.util.request.Field;
import io.piveau.hub.search.util.request.Query;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SearchRequestHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SearchRequestHelper.class);

    public static SearchRequest buildSearchRequest(Query query) {

        BoolQueryBuilder fullQuery = BuildQueryHelper.buildQuery(query);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(fullQuery);
        searchSourceBuilder.trackTotalHits(true);

        setRange(query, 10000, searchSourceBuilder);
        setInclude(query, searchSourceBuilder);

        SearchRequest searchRequest = new SearchRequest("dataset", "catalogue");
        searchRequest.source(searchSourceBuilder);

        return searchRequest;
    }

    public static SearchRequest buildSearchRequest(
            Query query,
            Integer maxResultWindow,
            Map<String, Field> fields,
            Map<String, ImmutablePair<String, String>> facets,
            Map<String, String> searchParams) {

        BoolQueryBuilder fullQuery = BuildQueryHelper.buildQuery(query, fields, facets, searchParams);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(fullQuery);
        searchSourceBuilder.trackTotalHits(true);

        setRange(query, maxResultWindow, searchSourceBuilder);
        setSort(query, fields, searchSourceBuilder);
        setInclude(query, searchSourceBuilder);

        SearchRequest searchRequest = new SearchRequest(query.getFilter());
        searchRequest.source(searchSourceBuilder);

        return searchRequest;
    }

    public static SearchRequest buildAggregationRequest(
            Query query,
            Integer maxAggSize,
            Map<String, Field> fields,
            Map<String, ImmutablePair<String, String>> facets,
            Map<String, String> searchParams) {

        BoolQueryBuilder fullQuery = BuildQueryHelper.buildQuery(query, fields, facets, searchParams);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(fullQuery);
        searchSourceBuilder.size(0);

        setAggregation(query, maxAggSize, facets, searchSourceBuilder);

        SearchRequest searchRequest = new SearchRequest(query.getFilter());
        searchRequest.source(searchSourceBuilder);

        return searchRequest;
    }

    private static void setRange(Query query, Integer maxResultWindow, SearchSourceBuilder searchSourceBuilder) {
        if (query.getFrom() + query.getSize() > maxResultWindow) {
            LOG.warn("from + size > max_result_window (" + maxResultWindow + ")");

            if (query.getFrom() > maxResultWindow) {
                LOG.warn("from > max_result_window; set from = 0 and size = 0");
                searchSourceBuilder.from(0);
                searchSourceBuilder.size(0);
            } else {
                LOG.warn("from <= max_result_window; set size = max_result_window - from = {}",
                        maxResultWindow - query.getFrom());
                searchSourceBuilder.from(query.getFrom());
                searchSourceBuilder.size(maxResultWindow - query.getFrom());
            }
        } else {
            searchSourceBuilder.from(query.getFrom());
            searchSourceBuilder.size(query.getSize());
        }
    }

    private static void setSort(Query query, Map<String, Field> fields, SearchSourceBuilder searchSourceBuilder) {
        List<ImmutablePair<String, SortOrder>> sort = new ArrayList<>();
        if (query.getSort() != null && !query.getSort().isEmpty() && fields != null) {
            for (String currentSort : query.getSort()) {
                String[] sortSplit = currentSort.split("\\+");

                String sortField = sortSplit[0];
                SortOrder sortOrder = SortOrder.DESC;

                if (sortSplit.length >= 2) {
                    if (sortSplit[1].toLowerCase().equals("asc")) {
                        sortOrder = SortOrder.ASC;
                    }
                }

                if (sortField.toLowerCase().equals("relevance")) {
                    sort.add(new ImmutablePair<>("relevance", sortOrder));
                } else {
                    String[] path = sortField.split("\\.");

                    Field result = checkSortField(fields.get(path[0]), path, 0);

                    if (result != null) {
                        if (result.getType() != null) {
                            if (result.getType().equals("text")) {
                                sort.add(new ImmutablePair<>(sortField + ".raw", sortOrder));
                            } else if (result.getType().equals("keyword") || result.getType().equals("date")) {
                                sort.add(new ImmutablePair<>(sortField, sortOrder));
                            }
                        }
                    }
                }
            }
        }

        if (!sort.isEmpty()) {
            for (ImmutablePair<String, SortOrder> sortPair : sort) {
                LOG.debug("sort - " + sortPair.getLeft() + " : " + sortPair.getRight().toString());
                if (sortPair.getLeft().equals("relevance")) {
                    searchSourceBuilder.sort(new ScoreSortBuilder().order((sortPair.getRight())));
                } else {
                    searchSourceBuilder.sort(new FieldSortBuilder(sortPair.getLeft()).order((sortPair.getRight())));
                }
            }
        }
    }

    private static void setAggregation(
            Query query,
            Integer maxAggSize,
            Map<String, ImmutablePair<String, String>> facets,
            SearchSourceBuilder searchSourceBuilder) {

        if (query.getGlobalAggregation()) {
            AggregationBuilder global = AggregationBuilders.global("global");

            for (String facetName : facets.keySet()) {
                if (query.isAggregationAllFields() || query.getAggregationFields().contains(facetName)) {
                    String facetTitle = facets.get(facetName).getKey();
                    String facetPath = facets.get(facetName).getValue();
                    global.subAggregation(
                            BuildQueryHelper.genTermsAggregation(facetPath, facetName, facetTitle, maxAggSize));
                }
            }

            searchSourceBuilder.aggregation(global);
        } else {
            for (String facetName : facets.keySet()) {
                if (query.isAggregationAllFields() || query.getAggregationFields().contains(facetName)) {
                    String facetTitle = facets.get(facetName).getKey();
                    String facetPath = facets.get(facetName).getValue();
                    searchSourceBuilder.aggregation(
                            BuildQueryHelper.genTermsAggregation(facetPath, facetName, facetTitle, maxAggSize)
                    );
                }
            }
        }
    }

    private static void setInclude(Query query, SearchSourceBuilder searchSourceBuilder) {
        List<String> includes = query.getIncludes();

        if (includes != null && !includes.isEmpty()) {
            String[] includesAsArray = includes.toArray(new String[0]);
            searchSourceBuilder.fetchSource(includesAsArray, null);
        }
    }

    private static Field checkSortField(Field current, String[] path, int i) {
        if (current == null) return null;

        if (path.length == ++i) {
            if (current.getSubFields() == null) {
                return current;
            } else {
                return null;
            }
        } else {
            if (current.getSubFields() != null) {
                Field result = null;
                for (Field subField : current.getSubFields()) {
                    if (subField.getName().equals(path[i])) {
                        result = checkSortField(subField, path, i);
                    }
                }
                return result;
            } else {
                return null;
            }
        }
    }

}
