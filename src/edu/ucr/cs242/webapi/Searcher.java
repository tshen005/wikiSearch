package edu.ucr.cs242.webapi;

import edu.ucr.cs242.Utility;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class Searcher {
    /**
     * The number of records to be batch-read per SQL transaction.
     */
    public static final int BATCH_READ_COUNT = 50;

    /**
     * The identifier for specifying category in the query.
     */
    private static final String CATEGORY_IDENTIFIER = "category:";

    /**
     * The number of search result shown in a page.
     */
    private static final int RESULT_PER_PAGE = 10;

    protected final Connection dbConnection;

    protected Searcher(String jdbcUrl) throws SQLException {
        this.dbConnection = DriverManager.getConnection(jdbcUrl);
    }

    private static String buildBatchSelectSQL(int numOfTitles) {
        // In a form of `SELECT title, content, categories, lastModify FROM pages WHERE title IN (?, ?, ?)`
        final String baseSQL = "SELECT title, content, categories, lastModify FROM pages WHERE title IN ";
        return baseSQL + IntStream.range(0, numOfTitles).mapToObj(i -> "?")
                .collect(Collectors.joining(", ", "(", ")"));
    }

    protected static String fullTextHighlight(String text, String keyword, String htmlTag) {
        List<String> keywordList = Utility.splitKeyword(keyword).stream()
                // Guarantee keywords are in lowercase
                .map(String::toLowerCase)
                .collect(Collectors.toList());

        String htmlBegin = "<" + htmlTag + ">", htmlEnd = "</" + htmlTag + ">";

        for (String word : keywordList) {
            Pattern pattern = Pattern.compile(Pattern.quote(word), Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(text);
            text = matcher.replaceAll(htmlBegin + "$0" + htmlEnd);
        }
        
        return text;
    }

    protected List<RelatedPage> fetchRelatedPages(Map<String, String> titleScoreMap, String keyword, String category,
                                                  BiFunction<String, String, String> fragmentHighlight) {
        // Keep the scored order from Lucene
        Map<String, RelatedPage> pages = new HashMap<>();
        List<String> titles = new ArrayList<>(titleScoreMap.keySet());

        int fetchCount = 0;
        while (fetchCount < titles.size()) {
            int localCount = 0;

            int batchSize = Math.min(titles.size() - fetchCount, BATCH_READ_COUNT);
            try (PreparedStatement statement = dbConnection.prepareStatement(buildBatchSelectSQL(batchSize))) {
                for (int i = 1; i <= batchSize; i++) {
                    statement.setString(i, titles.get(fetchCount + i - 1));
                }

                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        String title = result.getString("title");
                        String content = fragmentHighlight.apply(result.getString("content"), keyword);
                        List<String> categories =
                                Arrays.stream(result.getString("categories").split(Pattern.quote("|")))
                                .collect(Collectors.toList());
                        String lastMod = result.getString("lastModify");

                        pages.put(title, new RelatedPage(
                                fullTextHighlight(title, keyword, "span"),
                                title,
                                content,
                                categories.stream().map(s -> fullTextHighlight(s, category, "b")).collect(Collectors.toList()),
                                categories,
                                lastMod,
                                titleScoreMap.get(title)));
                        ++localCount;
                    }
                }

                fetchCount += localCount;
            } catch (Exception e) {
                System.out.println("Searcher::fetchRelatedPages throws an Exception.");
                e.printStackTrace();
            }
        }

        return titles.stream().map(pages::get).collect(Collectors.toList());
    }

    public final JSONObject search(String query, int pageId) {
        String keyword, category;

        int pos = query.indexOf(CATEGORY_IDENTIFIER);
        if (pos != -1) {
            keyword = query.substring(0, pos).trim();
            category = query.substring(pos + CATEGORY_IDENTIFIER.length()).trim();
        } else {
            keyword = query;
            category = "";
        }

        // Convert to lower case, since both Lucene and our algorithm is indexed in lower case.
        keyword = keyword.toLowerCase();
        category = category.toLowerCase();

        SearchResult result = searchInternal(keyword, category);
        JSONObject response = new JSONObject().put("hits", result.getNumOfHits());

        List<RelatedPage> pages = result.getRelatedPages();
        if (!pages.isEmpty()) {
            int pageLimit = (int) Math.ceil(pages.size() * 1.0f / RESULT_PER_PAGE);

            if (pageId < 0) {
                pageId = 0;
            } else if (pageId >= pageLimit) {
                pageId = pageLimit - 1;
            }

            JSONArray array = new JSONArray();
            pages.stream().skip(pageId * RESULT_PER_PAGE).limit(RESULT_PER_PAGE).forEach(p -> {
                JSONObject obj = new JSONObject();
                obj.put("title", p.getTitle());
                obj.put("url", "https://en.wikipedia.org/wiki/" + p.getRawTitle().replaceAll(" ", "_"));
                obj.put("snippet", p.getSnippet());
                obj.put("categories",
                        new JSONObject()
                                .put("html", p.getCategories())
                                .put("href", p.getRawCategories().stream()
                                        .map(s -> "https://en.wikipedia.org/wiki/Category:" + s.replaceAll(" ", "_"))
                                        .collect(Collectors.toList()))
                );
                obj.put("lastModify", p.getLastModify());
                obj.put("score", p.getScore());
                array.put(obj);
            });

            response.put("pages", array);
            response.put("pageNo", pageId + 1);
            response.put("totalPages", pageLimit);
        }

        return response;
    }

    protected abstract SearchResult searchInternal(String keyword, String category);
}
