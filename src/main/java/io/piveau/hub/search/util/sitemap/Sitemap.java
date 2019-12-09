package io.piveau.hub.search.util.sitemap;

import io.vertx.core.json.JsonArray;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name="sitemapindex")
public class Sitemap {

    public static class Url {
        @XmlElement
        private String loc;

        @XmlElement(name="xhtml:link")
        private List<Alternate> alternates;

        public Url(String loc, String language, JsonArray languages) {
            this.loc = loc + "?locale=" + language;

            this.alternates = new ArrayList<>();

            for (Object obj : languages) {
                String languageIterator = (String) obj;
                if (!language.equals(languageIterator)) {
                    addAlternate(languageIterator, loc + "?locale=" + languageIterator);
                }
            }
        }

        public List<Alternate> getAlternates() {
            return alternates;
        }

        public void addAlternate(String hreflang, String href) {
            this.alternates.add(new Alternate(hreflang, href));
        }
    }

    public static class Alternate {
        @XmlAttribute
        private String rel = "alternate";

        @XmlAttribute
        private String hreflang;

        @XmlAttribute
        private String href;

        public Alternate(String hreflang, String href) {
            this.hreflang = hreflang;
            this.href = href;
        }
    }

    @XmlAttribute
    private final String xmlns = "http://www.sitemaps.org/schemas/sitemap/0.9";

    @XmlAttribute(name="xmlns:xhtml")
    private String xmlns_xhtml = "http://www.w3.org/1999/xhtml";

    @XmlElement(name="url")
    private List<Url> urls;

    public Sitemap() {
        this.urls = new ArrayList<>();
    }

    public List<Url> getUrls() {
        return urls;
    }

    public void addSitemap(String loc, String language, JsonArray languages) {
        urls.add(new Url(loc, language, languages));
    }

}
