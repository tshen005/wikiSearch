package edu.ucr.cs242.webapi;

import edu.ucr.cs242.Utility;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class LuceneSearcher extends Searcher {
    private final Path indexPath;

    /**
     * Construct a Lucene searcher with given settings.
     * @param jdbcUrl   The JDBC url to the database.
     * @param indexPath The path to Lucene index.
     */
    public LuceneSearcher(String jdbcUrl, Path indexPath) throws SQLException {
        super(jdbcUrl);
        this.indexPath = indexPath;
    }

    // In PhraseQuery, order matters.
    private static BoostQuery buildPhraseQuery(String field, String keyword, int slop, float boost) {
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(slop);
        Utility.splitKeyword(keyword).forEach(s -> builder.add(new Term(field, s)));
        return new BoostQuery(builder.build(), boost);
    }

    private static BoostQuery buildKeywordQuery(String field, String keyword, BooleanClause.Occur occur, float boost) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        Utility.splitKeyword(keyword).forEach(s -> builder.add(new TermQuery(new Term(field, s)), occur));
        return new BoostQuery(builder.build(), boost);
    }

    private static String fragmentHighlight(String text, String keyword) {
        try {
            Query query = buildKeywordQuery("", keyword, BooleanClause.Occur.SHOULD, 1.0f);
            TokenStream tokenStream = new StandardAnalyzer().tokenStream("", text);
            Highlighter highlighter = new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(query));
            TextFragment[] fragments = highlighter.getBestTextFragments(tokenStream, text, false, 5);

            return Arrays.stream(fragments).filter(Objects::nonNull)
                    .map(TextFragment::toString)
                    // Replace all newlines with space
                    .map(s -> s.replaceAll("\\r\\n|\\r|\\n", " "))
                    // Replace multiple spaces into one space
                    .map(s -> s.replaceAll("[ ]+", " "))
                    .collect(Collectors.joining(" ... ", "... ", " ..."));
        } catch (InvalidTokenOffsetsException | IOException e) {
            return text;
        }
    }

    @Override
    protected SearchResult searchInternal(String keyword, String category) {
        try {
            Directory directory = FSDirectory.open(indexPath);
            DirectoryReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);
            BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();

            Query titleQuery = new BooleanQuery.Builder()
                    .add(buildPhraseQuery("title", keyword, 0, 20.0f), BooleanClause.Occur.SHOULD)
                    .add(buildKeywordQuery("title", keyword, BooleanClause.Occur.MUST, 5.0f), BooleanClause.Occur.SHOULD)
                    .add(new BooleanQuery.Builder()
                            .add(buildKeywordQuery("title", keyword, BooleanClause.Occur.SHOULD, 1.0f), BooleanClause.Occur.MUST)
                            .add(buildPhraseQuery("title", keyword, 0, 1.0f), BooleanClause.Occur.MUST_NOT)
                            .build(), BooleanClause.Occur.SHOULD)
                    .build();
            queryBuilder.add(titleQuery, BooleanClause.Occur.MUST);

            Query contentQuery = new BooleanQuery.Builder()
                    .add(buildPhraseQuery("content", keyword, 0, 2.0f), BooleanClause.Occur.SHOULD)
                    .add(buildKeywordQuery("content", keyword, BooleanClause.Occur.MUST, 1.05f), BooleanClause.Occur.SHOULD)
                    .add(new BooleanQuery.Builder()
                            .add(buildKeywordQuery("content", keyword, BooleanClause.Occur.SHOULD, 1.0f), BooleanClause.Occur.MUST)
                            .add(buildPhraseQuery("content", keyword, 0, 1.0f), BooleanClause.Occur.MUST_NOT)
                            .build(), BooleanClause.Occur.SHOULD)
                    .build();
            queryBuilder.add(new BoostQuery(contentQuery, 0.5f), BooleanClause.Occur.MUST);

            if (!category.isEmpty()) {
                Query categoryQuery = new BooleanQuery.Builder()
                        .add(buildPhraseQuery("categories", category, 0, 20.0f), BooleanClause.Occur.SHOULD)
                        .add(buildKeywordQuery("categories", category, BooleanClause.Occur.MUST, 5.0f), BooleanClause.Occur.SHOULD)
                        .add(new BooleanQuery.Builder()
                                .add(buildKeywordQuery("categories", category, BooleanClause.Occur.SHOULD, 1.0f), BooleanClause.Occur.MUST)
                                .add(buildPhraseQuery("categories", category, 0, 1.0f), BooleanClause.Occur.MUST_NOT)
                                .build(), BooleanClause.Occur.SHOULD)
                        .build();
                queryBuilder.add(categoryQuery, BooleanClause.Occur.MUST);
            }

            // Only get the top 1000 docs
            TopDocs topDocs = searcher.search(queryBuilder.build(), 1000);
            long hits = topDocs.totalHits;

            List<RelatedPage> pages = new ArrayList<>();
            if (hits > 0) {
                Map<String, String> titleScoreMap = Arrays.stream(topDocs.scoreDocs).map(sd -> {
                    try { return new AbstractMap.SimpleEntry<>(searcher.doc(sd.doc), sd.score); }
                    catch (IOException e) { return null; }
                }).filter(Objects::nonNull).collect(
                        // Supplier, LinkedHashMap keep the insertion order.
                        LinkedHashMap::new,
                        // Accumulator
                        (map, item) -> map.put(item.getKey().get("title"), String.valueOf(item.getValue())),
                        // Combiner
                        LinkedHashMap::putAll
                );

                pages = fetchRelatedPages(titleScoreMap, keyword, category, LuceneSearcher::fragmentHighlight);
            }

            reader.close();
            directory.close();
            return new SearchResult(hits, pages);
        } catch (Exception e) {
            System.out.println("LuceneSearcher throws an Exception.");
            e.printStackTrace();
            return null;
        }
    }
}
