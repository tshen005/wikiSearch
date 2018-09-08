package edu.ucr.cs242.webapi;

public class MixerScore {
    private final double totalScore;
    private final double bm25Score;
    private final double pageRank;

    public double getTotalScore() {
        return totalScore;
    }

    public double getBM25Score() {
        return bm25Score;
    }

    public double getPageRank() {
        return pageRank;
    }

    public MixerScore(double totalScore, double bm25Score, double pageRank) {
        this.totalScore = totalScore;
        this.bm25Score = bm25Score;
        this.pageRank = pageRank;
    }

    @Override
    public String toString() {
        if (pageRank < 0) {
            return String.format("%.8f", totalScore);
        } else {
            return String.format("%.8f (Normalized BM25 + Proximity: %.8f, Normalized PageRank: %.8f)", totalScore, bm25Score, pageRank);
        }
    }
}
