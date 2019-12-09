package io.piveau.hub.search.util.sitemap;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name="sitemapindex")
public class SitemapIndex {

    public static class Sitemap {
        @XmlElement
        private String loc;

        public Sitemap(String loc) {
            this.loc = loc;
        }
    }

    @XmlAttribute
    private final String xmlns = "http://www.sitemaps.org/schemas/sitemap/0.9";

    @XmlElement(name="sitemap")
    private List<Sitemap> sitemaps;

    public SitemapIndex() {
        this.sitemaps = new ArrayList<>();
    }

    public List<Sitemap> getSitemaps() {
        return sitemaps;
    }

    public void addSitemap(String loc) {
        sitemaps.add(new Sitemap(loc));
    }

}
