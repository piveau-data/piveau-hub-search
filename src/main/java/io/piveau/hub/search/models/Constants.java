package io.piveau.hub.search.models;

final public class Constants {

    static public final String ENV_VIADUCT_HUB_SEARCH_SERVICE_PORT = "VIADUCT_HUB_SEARCH_SERVICE_PORT";
    static public final String ENV_VIADUCT_HUB_SEARCH_BOOST = "VIADUCT_HUB_SEARCH_BOOST";
    static public final String ENV_VIADUCT_HUB_SEARCH_API_KEY = "VIADUCT_HUB_SEARCH_API_KEY";
    static public final String ENV_VIADUCT_HUB_SEARCH_ES_CONFIG = "VIADUCT_HUB_SEARCH_ES_CONFIG";
    static public final String ENV_VIADUCT_HUB_SEARCH_CLI_CONFIG = "VIADUCT_HUB_SEARCH_CLI_CONFIG";

    static public final String ENV_VIADUCT_HUB_SEARCH_GAZETTEER_CONFIG = "VIADUCT_HUB_SEARCH_GAZETTEER_CONFIG";

    static public final String ENV_VIADUCT_HUB_SEARCH_SITEMAP_CONFIG = "VIADUCT_HUB_SEARCH_SITEMAP_CONFIG";

    static public final String ENV_VIADUCT_HUB_SEARCH_TITLE = "VIADUCT_HUB_SEARCH_TITLE";

    public enum Operator {
        AND,
        OR
    }
}
