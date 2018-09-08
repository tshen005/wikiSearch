package edu.ucr.cs242.webapi;

import edu.ucr.cs242.Utility;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;

import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MixerSearcher extends Searcher {
    private static final double k1 = 1.2;
    private static final double k2 = 100.0;
    private static final double b = 0.75;

    private final DB levelDB;
    private final boolean withPageRank;
    private final SnowballStemmer stemmer = new englishStemmer();

    // Only 3 field, 0 - title, 1 - content, 2 - categories
    private final double[] avgDocLength = new double[3];
    private final double numberOfDocs;
    private final double maxPageRank;

    /**
     * Construct a Lucene searcher with given settings.
     * @param jdbcUrl      The JDBC url to the database.
     * @param levelDB      The LevelDB object.
     * @param withPageRank Whether take PageRank into account.
     */
    public MixerSearcher(String jdbcUrl, DB levelDB, boolean withPageRank) throws SQLException {
        super(jdbcUrl);
        this.levelDB = levelDB;
        this.withPageRank = withPageRank;

        numberOfDocs = Double.parseDouble(Utility.levelDBGet(levelDB, "__docCount"));
        maxPageRank = Double.parseDouble(Utility.levelDBGet(levelDB, "__docMaxPR"));
        for (int i = 0; i < avgDocLength.length; i++) {
            avgDocLength[i] = Double.parseDouble(Utility.levelDBGet(levelDB, "__avgDocLength_" + i));
        }
    }

    private List<String> getQueryTerms(String query) {
        return Utility.splitKeyword(query).stream()
                .filter(s -> !Utility.isStopWord(s))
                .map(s -> {
                    stemmer.setCurrent(s);
                    stemmer.stem();
                    return stemmer.getCurrent();
                }).collect(Collectors.toList());
    }

    // <Term, QueryFreq>
    private Map<String, Integer> getQueryFrequency(List<String> terms) {
        return terms.stream().distinct()
                .collect(Collectors.toMap(Function.identity(), t -> Collections.frequency(terms, t)));
    }

    // <Term, <DocId, InvertedIndex>>
    private Map<String, Map<Integer, MixerInvertedIndex>> fetchInvertedIndex(Set<String> terms) {
        return terms.stream().map(t -> {
            try {
                String value = Utility.levelDBGet(levelDB, t);
                if (value != null) {
                    Map<Integer, MixerInvertedIndex> indexMap = new HashMap<>();

                    for (Object el : new JSONArray(value)) {
                        JSONObject json = (JSONObject) el;

                        int docId = Integer.parseInt(json.keys().next());
                        JSONObject index = json.getJSONObject(String.valueOf(docId));
                        JSONArray posArray = index.getJSONArray("position");

                        List<Integer> freq = new ArrayList<>();
                        List<List<Integer>> pos = new ArrayList<>();

                        int fieldId = 0;
                        for (Object f : index.getJSONArray("frequency")) {
                            Integer fi = (Integer) f;
                            freq.add(fi);

                            List<Integer> posList = new ArrayList<>();
                            for (Object p : posArray.getJSONArray(fieldId)) {
                                posList.add((Integer) p);
                            }
                            pos.add(posList);

                            ++fieldId;
                        }

                        indexMap.put(docId, new MixerInvertedIndex(docId, freq, pos));
                    }

                    return new AbstractMap.SimpleEntry<>(t, indexMap);
                }

                return null;
            } catch (JSONException e) {
                return null;
            } catch (DBException e) {
                System.err.println("MixerSearcher::searchInternal throws a DBException.");
                e.printStackTrace();
                return null;
            }
        }).filter(Objects::nonNull).collect(
                Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)
        );
    }

    // Calculate BM25 for a single term
    private double BM25(double termFreq, double queryFreq, double docFreq, double docLength, double avgDocLength) {
        double K = k1 * ((1 - b) + b * docLength / avgDocLength);
        double part1 = Math.log((numberOfDocs - docFreq + 0.5) / (docFreq + 0.5));
        double part2 = Math.log((k1 + 1) * termFreq / (K + termFreq));
        double part3 = Math.log((k2 + 1) * queryFreq / (k2 + queryFreq));
        return part1 + part2 + part3;
    }

    // <docId, score>
    private Map<Integer, Double> scoreTerm(int fieldId, double queryFreq, Map<Integer, MixerInvertedIndex> termIndex) {
        Map<Integer, Double> score = new HashMap<>();

        long docFreq = termIndex.values().stream()
                .filter(index -> index.getFrequency().get(fieldId) > 0).count();

        termIndex.forEach((docId, index) -> {
            int docLength = Integer.parseInt(Utility.levelDBGet(levelDB, "__docLength_" + docId + "_" + fieldId));

            double bm25 = BM25(index.getFrequency().get(fieldId), queryFreq, docFreq, docLength, avgDocLength[fieldId]);
            // Filter out invalid result.
            if (bm25 != Double.POSITIVE_INFINITY && bm25 != Double.NEGATIVE_INFINITY && bm25 != Double.NaN) {
                score.put(docId, bm25);
            }
        });

        return score;
    }

    // <docId, <keyword, score>>
    private Map<Integer, Map<String, Double>> scoreTerms(int fieldId, Map<String, Integer> queryFreq,
                                                         Map<String, Map<Integer, MixerInvertedIndex>> invertedIndex) {
        Map<Integer, Map<String, Double>> retMap = new HashMap<>();

        for (Map.Entry<String, Map<Integer, MixerInvertedIndex>> entry : invertedIndex.entrySet()) {
            String term = entry.getKey();
            scoreTerm(fieldId, queryFreq.get(term), entry.getValue()).forEach((docId, score) -> {
                if (!retMap.containsKey(docId)) {
                    retMap.put(docId, new HashMap<>());
                }

                retMap.get(docId).put(term, score);
            });
        }

        return retMap;
    }

    // <DocId, Score>
    private Map<Integer, Double> calculateFieldScore(int fieldId,
                                                     Map<Integer, Map<String, Double>> termScore,
                                                     Map<String, Map<Integer, MixerInvertedIndex>> invertedIndex,
                                                     List<String> queryTerms,
                                                     Map<String, Integer> queryFrequency,
                                                     double exactMatchBoost, double orderMatchBoost, double allOccurBoost,
                                                     double partialMatchBoost, double togetherBoost) {
        Map<Integer, Double> finalScore = new HashMap<>();

        termScore.forEach((docId, entry) -> {
            double sumScore = entry.values().stream().collect(Collectors.summarizingDouble(v -> v)).getSum();

            // All terms occurred in the document?
            if (entry.size() == queryFrequency.size()) {
                // Bi-gram for order match
                int orderMatchCount = 0;
                for (int i = 0; i < queryTerms.size() - 1; i++) {
                    String prevTerm = queryTerms.get(i), nextTerm = queryTerms.get(i + 1);

                    List<Integer> prevPos = invertedIndex.get(prevTerm).get(docId).getPosition().get(fieldId);
                    List<Integer> nextPos = invertedIndex.get(nextTerm).get(docId).getPosition().get(fieldId);

                    if (prevPos.stream().map(pos -> pos + 1).anyMatch(nextPos::contains)) {
                        ++orderMatchCount;
                    }
                }

                if (orderMatchCount == queryTerms.size() - 1) {
                    // Exact match?
                    int docLength = Integer.parseInt(Utility.levelDBGet(levelDB, "__docLength_" + docId + "_" + fieldId));
                    if (docLength == queryTerms.size()) {
                        sumScore *= exactMatchBoost;
                    } else {
                        sumScore *= orderMatchBoost;
                    }
                } else {
                    sumScore *= allOccurBoost;
                }
            } else {
                sumScore *= partialMatchBoost;
            }

            finalScore.put(docId, sumScore * togetherBoost);
        });

        return finalScore;
    }

    // <DocId, Score>
    private Map<Integer, Double> combineFieldScore(Map<Integer, Double> score1, Map<Integer, Double> score2,
                                                   boolean docMustOccurInBoth) {

        // Efficient concern.
        Map<Integer, Double> small = score1.size() < score2.size() ? score1 : score2;
        Map<Integer, Double> big = score1.size() < score2.size() ? score2 : score1;

        Map<Integer, Double> retMap;
        if (docMustOccurInBoth) {
            retMap = new HashMap<>();
            small.forEach((k, v) -> {
                if (big.containsKey(k)) {
                    retMap.put(k, v + big.get(k));
                }
            });
        } else {
            retMap = new HashMap<>(big);
            small.forEach((k, v) -> retMap.merge(k, v, (a, b) -> a + b));
        }

        return retMap;
    }

    private Map.Entry<Integer, MixerScore> combinePageRank(Integer docId, Double bm25Score) {
        if (!withPageRank) {
            return new AbstractMap.SimpleEntry<>(docId, new MixerScore(bm25Score, bm25Score, -1));
        } else {
            String rawPageRank = Utility.levelDBGet(levelDB, "__docPR_" + docId);
            Double pageRank = 1.0 / numberOfDocs; // The initial PageRank

            // Does this page has a PageRank value?
            if (rawPageRank != null) {
                pageRank = Double.parseDouble(rawPageRank);
            }

            // Normalize PageRank with Max-min normalization
            Double normalizedPR = normalize(pageRank, 0.0, maxPageRank, 0.0, 1000.0);
            // BM25 * 0.8 + PR * 0.2 is the final score
            return new AbstractMap.SimpleEntry<>(docId,
                    new MixerScore(bm25Score * 0.9 + normalizedPR * 0.1, bm25Score, normalizedPR));
        }
    }

    private double normalize(double original, double min, double max, double newMin, double newMax) {
        if (min == max)
            return newMax;

        return (original - min) / (max - min) * (newMax - newMin) + newMin;
    }

    private static String fragmentHighlight(String text, String keyword) {
        String[] sentences = text.split("[,;.\n]");
        int[] sentenceScore = new int[sentences.length];

        List<String> keywordList = Utility.splitKeyword(keyword).stream()
                // Guarantee keywords are in lowercase
                .map(String::toLowerCase)
                .collect(Collectors.toList());

        List<Pattern> patterns = keywordList.stream()
                .map(w -> Pattern.compile("[ ]+" + w + "[ ]+", Pattern.CASE_INSENSITIVE))
                .collect(Collectors.toList());

        IntStream.range(0, sentences.length).forEach(i -> {
            int[] wordCount = new int[keywordList.size()];
            List<Matcher> matchers = patterns.stream().map(p -> p.matcher(sentences[i])).collect(Collectors.toList());
            IntStream.range(0, matchers.size()).forEach(j -> { while (matchers.get(j).find()) { ++wordCount[j]; } });
            // We prefer the sentence with all key words at least showing 1 time.
            sentenceScore[i] = Arrays.stream(wordCount).min().orElse(0) * 20 + Arrays.stream(wordCount).sum();
        });

        return IntStream.range(0, sentenceScore.length)
                .mapToObj(i -> new AbstractMap.SimpleEntry<>(i, sentenceScore[i]))
                // Ignore no occurrences
                .sorted((a, b) -> b.getValue() - a.getValue())
                .limit(5) // 5 sentences
                .map(p -> sentences[p.getKey()])
                .map(s -> Searcher.fullTextHighlight(s, keyword, "b"))
                // Replace all newlines with space
                .map(s -> s.replaceAll("\\r\\n|\\r|\\n", " "))
                // Replace multiple spaces into one space
                .map(s -> s.replaceAll("[ ]+", " "))
                .collect(Collectors.joining(" ... ", "... ", " ..."));
    }

    @Override
    protected SearchResult searchInternal(String keyword, String category) {
        try {
            List<String> keywordQueryTerms = getQueryTerms(keyword);
            Map<String, Integer> keywordQueryFreq = getQueryFrequency(keywordQueryTerms);
            Map<String, Map<Integer, MixerInvertedIndex>> keywordInvertedIndex = fetchInvertedIndex(keywordQueryFreq.keySet());

            int hits = 0;
            List<RelatedPage> pages = new ArrayList<>();

            // Get some keyword hits?
            if (!keywordInvertedIndex.isEmpty()) {
                // 0 - title
                Map<Integer, Map<String, Double>> titleTermScore = scoreTerms(0, keywordQueryFreq, keywordInvertedIndex);
                // 1 - content
                Map<Integer, Map<String, Double>> contentTermScore = scoreTerms(1, keywordQueryFreq, keywordInvertedIndex);

                Map<Integer, Double> titleScore = calculateFieldScore(0, titleTermScore,
                        keywordInvertedIndex, keywordQueryTerms, keywordQueryFreq,
                        20.0f, 10.0f, 5.0f, 1.0f, 1.0f);
                Map<Integer, Double> contentScore = calculateFieldScore(1, contentTermScore,
                        keywordInvertedIndex, keywordQueryTerms, keywordQueryFreq,
                        2.0f, 1.2f, 1.05f, 1.0f, 0.5f);
                Map<Integer, Double> finalScore = combineFieldScore(titleScore, contentScore, true);

                if (!category.isEmpty()) {
                    List<String> categoryQueryTerms = getQueryTerms(category);
                    Map<String, Integer> categoryQueryFreq = getQueryFrequency(categoryQueryTerms);
                    Map<String, Map<Integer, MixerInvertedIndex>> categoryInvertedIndex = fetchInvertedIndex(categoryQueryFreq.keySet());

                    // 2 - category
                    Map<Integer, Map<String, Double>> categoryTermScore = scoreTerms(2, categoryQueryFreq, categoryInvertedIndex);

                    Map<Integer, Double> categoryScore = calculateFieldScore(2, categoryTermScore,
                            categoryInvertedIndex, categoryQueryTerms, categoryQueryFreq,
                            20.0f, 10.0f, 5.0f, 1.0f, 1.0f);

                    finalScore = combineFieldScore(finalScore, categoryScore, true);
                }

                // Get any hits?
                if (!finalScore.isEmpty()) {
                    hits = finalScore.size();

                    if (withPageRank) {
                        // Normalization the BM25 score (with Min-max normalization)
                        DoubleSummaryStatistics stat = finalScore.values().stream().collect(Collectors.summarizingDouble(s -> s));
                        for (Integer docId : finalScore.keySet()) {
                            finalScore.put(docId, normalize(finalScore.get(docId), stat.getMin(), stat.getMax(), 0.0, 100.0));
                        }
                    }

                    Map<String, String> titleScoreMap = finalScore.entrySet().stream()
                            // Adding PageRank
                            .map(entry -> combinePageRank(entry.getKey(), entry.getValue()))
                            // Max to min
                            .sorted((a, b) -> Double.compare(b.getValue().getTotalScore(), a.getValue().getTotalScore()))
                            // Only top 1000 results
                            .limit(1000)
                            // LinkedHashMap keep the insertion order.
                            .collect(LinkedHashMap::new, // Supplier
                                    // Accumulator
                                    (map, item) -> map.put(
                                            Utility.levelDBGet(levelDB, "__docId_" + item.getKey()),
                                            item.getValue().toString()
                                    ),
                                    // Combiner
                                    LinkedHashMap::putAll
                            );

                    pages = fetchRelatedPages(titleScoreMap, keyword, category, MixerSearcher::fragmentHighlight);
                }
            }

            return new SearchResult(hits, pages);
        } catch (Exception e) {
            System.out.println("MixerSearcher throws an Exception");
            e.printStackTrace();
            return null;
        }
    }
}
