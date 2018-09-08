package edu.ucr.cs242.webapi;

import java.util.List;

public class SearchResult {
    private long numOfHits;
    private List<RelatedPage> relatedPages;

    public long getNumOfHits() {
        return numOfHits;
    }

    public List<RelatedPage> getRelatedPages() {
        return relatedPages;
    }

    /**
     * Represent the search result.
     * @param numOfHits    The number of hits.
     * @param relatedPages The list of related pages.
     */
    public SearchResult(long numOfHits, List<RelatedPage> relatedPages) {
        this.numOfHits = numOfHits;
        this.relatedPages = relatedPages;
    }
}
