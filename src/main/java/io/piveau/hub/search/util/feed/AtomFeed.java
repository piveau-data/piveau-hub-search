package io.piveau.hub.search.util.feed;

import io.vertx.core.json.JsonObject;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

@XmlRootElement(name="feed")
public class AtomFeed {

    public static class Author {
        @XmlElement
        private String name;
        @XmlElement
        private String uri;

        public Author(String name, String uri) {
            this.name = name;
            this.uri = uri;
        }
    }

    public static class Summary {
        @XmlAttribute
        private String type;
        @XmlValue
        private String content;

        public Summary(String content, String type) {
            this.content = content;
            this.type = type;
        }
    }

    public static class Link {
        @XmlAttribute
        private String href;
        @XmlAttribute
        private String rel;
        @XmlAttribute
        private String type;

        public Link(String href, String rel, String type) {
            this.href = href;
            this.rel = rel;
            this.type = type;
        }
    }

    public static class Entry {
        @XmlElement
        private String id;
        @XmlElement
        private String title;
        @XmlElement
        private Summary summary;
        @XmlElement(name="link")
        private List<Link> links;
        @XmlElement
        private String published;
        @XmlElement
        private String updated;
        @XmlElement(name="link")
        private Link alternate;

        public Entry(String id, String title, String summary_content, String summary_type, String published,
                     String updated, String alternate_href) {
            this.id = id;
            this.title = title;
            this.summary = new Summary(summary_content, summary_type);
            this.links = new ArrayList<>();
            this.published = published;
            this.updated = updated;
            this.alternate = new Link(alternate_href, "alternate", null);
        }

        public void addLink(String href, String rel, String type) {
            links.add(new Link(href, rel, type));
        }
    }

    @XmlAttribute
    private final String xmlns = "http://www.w3.org/2005/Atom";
    @XmlAttribute(name="xml:lang")
    private String xml_lang;
    @XmlElement
    private String title;
    @XmlElement(name="link")
    private Link alternate;
    @XmlElement(name="link")
    private Link self;
    @XmlElement
    private String id;
    @XmlElement
    private String updated;
    @XmlElement
    private Author author;
    @XmlElement
    private String subtitle;
    @XmlElement(name="link")
    private Link first;
    @XmlElement(name="link")
    private Link next;
    @XmlElement(name="link")
    private Link previous;
    @XmlElement(name="link")
    private Link last;
    @XmlElement(name="entry")
    private List<Entry> entries;

    public AtomFeed() {
        this.xml_lang = "";
        this.title = "";
        this.entries = new ArrayList<>();
        this.alternate = null;
        this.self = null;
        this.id = "";
        this.updated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date());
        this.author = null;
        this.subtitle = "";
        this.first = null;
        this.next = null;
        this.previous = null;
        this.last = null;
    }

    public AtomFeed(String xml_lang, String title, String id, String author_name, String author_uri,
                    String subtitle, JsonObject links) {
        this.xml_lang = xml_lang;
        this.title = title + " - Atom Feed";
        this.entries = new ArrayList<>();
        this.alternate = new Link(links.getString("alternate"), "alternate", null);
        this.self = new Link(links.getString("self"), "self", null);
        this.id = id;
        this.updated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date());
        this.author = new Author(author_name, author_uri);
        this.subtitle = subtitle;
        this.first = new Link(links.getString("first"), "first", null);
        if (links.getString("next") != null) {
            this.next = new Link(links.getString("next"), "next", null);
        } else {
            this.next = null;
        }
        if (links.getString("previous") != null) {
            this.previous = new Link(links.getString("previous"), "previous", null);
        } else {
            this.previous = null;
        }
        this.last = new Link(links.getString("last"), "last", null);
    }

    public Entry addEntry(String id, String title, String summary_content, String summary_type, String published,
                          String updated, String alternate_href) {
        Entry entry = new Entry(id, title, summary_content, summary_type, published, updated, alternate_href);

        entries.add(entry);

        return entry;
    }
}
