package edu.ucr.cs242.webapi;

import java.util.List;

public class RelatedPage {
    private final String title;
    private final String rawTitle;
    private final String snippet;
    private final List<String> categories;
    private final List<String> rawCategories;
    private final String lastModify;
    private final String score;

    public String getTitle() {
        return title;
    }

    public String getRawTitle() {
        return rawTitle;
    }

    public String getSnippet() {
        return snippet;
    }

    public List<String> getCategories() {
        return categories;
    }

    public List<String> getRawCategories() {
        return rawCategories;
    }

    public String getLastModify() {
        return lastModify;
    }

    public String getScore() { return score; }

    /**
     * Represent a related page.
     * @param title         The page title.
     * @param rawTitle      The raw title to the page (no b tags include).
     * @param snippet       The page snippet.
     * @param categories    The categories the page belongs to.
     * @param rawCategories The raw categories the page belongs to (no b tags include).
     * @param lastModify    The last modification time of the page.
     * @param score         The score of the page.
     */
    public RelatedPage(String title, String rawTitle, String snippet,
                       List<String> categories, List<String> rawCategories, String lastModify,
                       String score) {
        this.title = title;
        this.rawTitle = rawTitle;
        this.snippet = snippet;
        this.categories = categories;
        this.rawCategories = rawCategories;
        this.lastModify = lastModify;
        this.score = score;
    }
}
