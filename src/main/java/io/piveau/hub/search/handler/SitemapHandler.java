package io.piveau.hub.search.handler;

import io.piveau.hub.search.util.sitemap.Sitemap;
import io.piveau.hub.search.util.sitemap.SitemapIndex;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;

public class SitemapHandler extends SearchHandler {

    private JsonObject config;

    public SitemapHandler(JsonObject config, Vertx vertx, String address) {
        super(vertx, address);
        this.config = config;
    }

    public void getMeta(RoutingContext context)  {
        JsonObject query = new JsonObject();
        query.put("size", 0);
        query.put("filter", "dataset");
        query.put("aggregation", false);
        searchService.search(query.toString(), ar -> {
            if (ar.succeeded()) {
                Integer countDatasets = ar.result().getJsonObject("result").getInteger("count");
                String sitemapIndex = generateSitemapIndex(countDatasets);
                context.response().putHeader("Content-Type", "application/atom+xml");
                context.response().setStatusCode(200);
                context.response().end(sitemapIndex);
            } else {
                JsonObject response = new JsonObject();
                context.response().putHeader("Content-Type", "application/json");
                context.response().putHeader("Access-Control-Allow-Origin", "*");
                response.put("success", false);
                JsonObject result = new JsonObject(ar.cause().getMessage());
                response.put("result", result);
                Integer status = (Integer) result.remove("status");
                context.response().setStatusCode(status);
                context.response().end(response.toString());
            }
        });
    }

    public void getDatasets(RoutingContext context) {
        int id = Integer.parseInt(context.request().getParam("id"));
        JsonObject query = new JsonObject();
        query.put("from", (id-1)*config.getInteger("size"));
        query.put("size", config.getInteger("size"));
        query.put("filter", "dataset");
        query.put("aggregation", false);
        query.put("includes", new JsonArray().add("id")); //.add("catalog"));
        searchService.search(query.toString(), ar -> {
            if (ar.succeeded()) {
                JsonArray entries = ar.result().getJsonObject("result").getJsonArray("results");
                String sitemapIndex = generateSitemap("datasets", entries);
                context.response().putHeader("Content-Type", "application/atom+xml");
                context.response().setStatusCode(200);
                context.response().end(sitemapIndex);
            } else {
                JsonObject response = new JsonObject();
                context.response().putHeader("Content-Type", "application/json");
                context.response().putHeader("Access-Control-Allow-Origin", "*");
                response.put("success", false);
                JsonObject result = new JsonObject(ar.cause().getMessage());
                response.put("result", result);
                Integer status = (Integer) result.remove("status");
                context.response().setStatusCode(status);
                context.response().end(response.toString());
            }
        });
    }

    private String generateSitemapIndex(int countDatasets) {

        SitemapIndex sitemapIndex = new SitemapIndex();

        sitemapIndex.addSitemap(config.getString("drupal", ""));

        int count = countDatasets / config.getInteger("size");

        if (countDatasets % config.getInteger("size") != 0) {
            count++;
        }

        for (int i = 0; i < count; ++i) {
            sitemapIndex.addSitemap(config.getString("url", "") + "sitemap_datasets_" + (i + 1) + ".xml");
        }

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(SitemapIndex.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            // output pretty printed
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            StringWriter sw = new StringWriter();
            jaxbMarshaller.marshal(sitemapIndex, sw);
            // DEBUG: jaxbMarshaller.marshal(feed, System.out);

            return sw.toString();
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String generateSitemap(String type, JsonArray entries) {

        Sitemap sitemap = new Sitemap();

        for(Object obj : entries) {
            JsonObject entry = (JsonObject) obj;

            String language = "en";

            /*JsonObject catalog = entry.getJsonObject("catalog");

            if (catalog != null) {
                JsonArray languages = catalog.getJsonArray("languages");
                if (languages != null && !languages.isEmpty()) {
                    language = languages.getString(0);
                }
            }*/

            sitemap.addSitemap(config.getString("url", "") + type + "/" +
                    entry.getString("id"), language, config.getJsonArray("languages", new JsonArray()));
        }

        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(Sitemap.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            // output pretty printed
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

            StringWriter sw = new StringWriter();
            jaxbMarshaller.marshal(sitemap, sw);
            // DEBUG: jaxbMarshaller.marshal(feed, System.out);

            return sw.toString();
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }
}
