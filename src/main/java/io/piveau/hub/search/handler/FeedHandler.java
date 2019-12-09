package io.piveau.hub.search.handler;

import io.piveau.hub.search.util.feed.AtomFeed;
import io.piveau.hub.search.util.feed.RSSFeed;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;

public class FeedHandler extends SearchHandler {

    private String title;

    public FeedHandler(String title, Vertx vertx, String address) {
        super(vertx, address);
        this.title = title;
    }

    private String getFieldByLang(JsonObject field, String lang) {
        String field_lang = null;
        if (field != null) {
            if (field.getString(lang) != null) {
                field_lang = field.getString(lang);
            } else if (field.getString("en") != null) {
                field_lang = field.getString("en");
            } else {
                field_lang = "not available in the requested language / english";
            }
        }
        return field_lang;
    }

    private String genarateAtomFeed(JsonObject result, String uri, String absoluteUri, String path, Integer pageInt,
                                    Integer limitInt, String lang) {
        Integer count = result.getInteger("count");

        Integer last = Math.max(count / limitInt - 1, 0);

        JsonObject links = new JsonObject();

        boolean containsPage = absoluteUri.contains("page=");
        boolean endsWithAnd = absoluteUri.endsWith("&");

        links.put("self", absoluteUri);

        if (endsWithAnd) {
            links.put("alternate", absoluteUri.replace(path, "/search") + "filter=dataset");
        } else{
            links.put("alternate", absoluteUri.replace(path, "/search") + "&filter=dataset");
        }

        links.put("first", absoluteUri.replaceAll("page=[0-9]+", "page=0"));

        if (pageInt != 0) {
            links.put("previous", absoluteUri.replaceAll("page=[0-9]+", "page=" + (pageInt - 1)));
        }

        if (pageInt < last) {
            if(containsPage) {
                links.put("next", absoluteUri.replaceAll("page=[0-9]+", "page=" + (pageInt + 1)));
            } else {
                if (endsWithAnd) {
                    links.put("next", absoluteUri.concat("page=2"));
                } else {
                    links.put("next", absoluteUri.concat("&page=2"));
                }
            }
        }

        if(containsPage) {
            links.put("last", absoluteUri.replaceAll("page=[0-9]+", "page=" + last));
        } else {
            if (endsWithAnd) {
                links.put("last", absoluteUri.concat("page=" + last));
            } else {
                links.put("last", absoluteUri.concat("&page=" + last));
            }
        }

        AtomFeed atomFeed = new AtomFeed(
                lang,
                title,
                uri + path,
                "default",
                uri,
                "",
                links
        );

        result.getJsonArray("results").forEach(dataset -> {
            JsonObject datasetJson = new JsonObject(dataset.toString());

            String id = datasetJson.getString("id");

            JsonObject title = datasetJson.getJsonObject("title");
            JsonObject description = datasetJson.getJsonObject("description");

            String title_lang = getFieldByLang(title, lang);
            String description_lang = getFieldByLang(description, lang);

            AtomFeed.Entry entry = atomFeed.addEntry(
                    uri + "/datasets/" + id,
                    title_lang,
                    description_lang,
                    "html",
                    datasetJson.getString("release_date"),
                    datasetJson.getString("modification_date"),
                    uri + "/datasets/" + id
            );

            JsonArray distributions = datasetJson.getJsonArray("distributions");
            if (distributions != null) {
                distributions.forEach(dist -> {
                    JsonObject distJson = new JsonObject(dist.toString());

                    JsonObject format = distJson.getJsonObject("format");
                    String access_url = distJson.getString("access_url");

                    if (format != null && access_url != null && format.getString("id") != null &&
                            !format.getString("id").isEmpty() && !access_url.isEmpty()) {

                        entry.addLink(access_url, "enclosure", format.getString("id"));
                    }
                });
            }
        });

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(AtomFeed.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            // output pretty printed
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            StringWriter sw = new StringWriter();
            jaxbMarshaller.marshal(atomFeed, sw);
            // DEBUG: jaxbMarshaller.marshal(feed, System.out);

            return sw.toString();
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String generateRSSFeed(JsonObject result, String uri, String absoluteUri, String path, String lang) {
        boolean endsWithAnd = absoluteUri.endsWith("&");

        String link;
        if (endsWithAnd) {
            link = absoluteUri.replace(path, "/search") + "filter=dataset";
        } else {
            link = absoluteUri.replace(path, "/search") + "&filter=dataset";
        }

        RSSFeed rssFeed = new RSSFeed(
                title,
                link,
                "",
                lang,
                "default"
        );

        result.getJsonArray("results").forEach(dataset -> {
            JsonObject datasetJson = new JsonObject(dataset.toString());

            String id = datasetJson.getString("id");

            JsonObject title = datasetJson.getJsonObject("title");
            JsonObject description = datasetJson.getJsonObject("description");

            String title_lang = getFieldByLang(title, lang);
            String description_lang = getFieldByLang(description, lang);

            // RSS has no field for modification date, we use pubdate instead.
            RSSFeed.Item item = rssFeed.addItem(
                    uri + "/datasets/" + id,
                    title_lang,
                    uri + "/datasets/" + id,
                    description_lang,
                    datasetJson.getString("modification_date")
            );

            JsonArray distributions = datasetJson.getJsonArray("distributions");
            if (distributions != null) {
                distributions.forEach(dist -> {
                    JsonObject distJson = new JsonObject(dist.toString());

                    JsonObject format = distJson.getJsonObject("format");
                    String access_url = distJson.getString("access_url");

                    if (format != null && access_url != null && format.getString("id") != null &&
                            !format.getString("id").isEmpty() && !access_url.isEmpty()) {

                        item.addEnclosure(access_url, format.getString("id"));
                    }
                });
            }
        });

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(RSSFeed.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            // output pretty printed
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            StringWriter sw = new StringWriter();
            jaxbMarshaller.marshal(rssFeed, sw);
            // DEBUG: jaxbMarshaller.marshal(feed, System.out);

            return sw.toString();
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void feed(RoutingContext context, String type) {
        MultiMap params = context.request().params();

        JsonObject query = paramsToQuery(params);

        query.put("filter", "dataset");
        query.put("aggregation", false);

        if (query.getJsonArray("sort") == null || query.getJsonArray("sort").isEmpty()) {
            query.put("sort", new JsonArray().add("modification_date+desc"));
        }

        String lang = params.get("lang");

        String q = params.get("q");
        if (q != null)
            query.put("q", q);

        String uri = context.request().absoluteURI().replace(context.request().uri(), "");
        String absoluteUri = context.request().absoluteURI();
        String path = context.request().path();

        context.response().putHeader("Access-Control-Allow-Origin", "*");
        searchService.search(query.toString(), ar -> {
            if (ar.succeeded()) {
                JsonObject result = ar.result().getJsonObject("result");

                int pageInt;
                try {
                    pageInt = Integer.parseInt(query.getString("page"));
                } catch (NumberFormatException e) {
                    pageInt = 0;
                }

                int limitInt;
                try {
                    limitInt = Integer.parseInt(query.getString("limit"));
                } catch (NumberFormatException e) {
                    limitInt = 10;
                }

                String feed = "";

                if(type.equals("atom")) {
                    feed = genarateAtomFeed(result, uri, absoluteUri, path, pageInt, limitInt, lang);
                }

                if(type.equals("rss")) {
                    feed = generateRSSFeed(result, uri, absoluteUri, path, lang);
                }

                if (feed != null && !feed.isEmpty()) {
                    context.response().putHeader("Content-Type", "application/atom+xml");
                    context.response().setStatusCode(200);
                    context.response().end(feed);
                } else {
                    JsonObject response = new JsonObject();
                    response.put("success", false);
                    response.put("message", "Feed marshal error.");
                    context.response().putHeader("Content-Type", "application/json");
                    context.response().setStatusCode(500);
                    context.response().end(response.toString());
                }
            } else {
                JsonObject response = new JsonObject();
                response.put("success", false);
                JsonObject result = new JsonObject(ar.cause().getMessage());
                response.put("result", result);
                Integer status = (Integer) result.remove("status");
                context.response().putHeader("Content-Type", "application/json");
                context.response().setStatusCode(status);
                context.response().end(response.toString());
            }
        });
    }

    public void atom(RoutingContext context) {
        feed(context, "atom");
    }

    public void rss(RoutingContext context) {
        feed(context, "rss");
    }
}
