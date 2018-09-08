package edu.ucr.cs242.mixer.importer;

import edu.ucr.cs242.Utility;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.json.JSONObject;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class DocumentLengthImportThread extends Thread {
    private final DB database;
    private final String jsonOutputPath;

    /**
     * Construct a document length import thread, with given settings.
     * @param database       The LevelDB object.
     * @param jsonOutputPath The folder to the JSON output.
     */
    public DocumentLengthImportThread(DB database, String jsonOutputPath) {
        this.database = database;
        this.jsonOutputPath = jsonOutputPath;
    }

    private long putLength(int docId, int fieldId, String text) {
        long length = new StringTokenizer(text).countTokens();

        // <docId, length>
        database.put(JniDBFactory.bytes("__docLength_" + docId + "_" + fieldId),
                JniDBFactory.bytes(String.valueOf(length)));

        return length;
    }

    @Override
    public void run() {
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("DocumentLengthImportThread started at " + startAt.toLocalTime() + ".");

        int indexedCount = 0;

        try (BufferedReader dataReader = new BufferedReader(
                new FileReader(Paths.get(jsonOutputPath, "data.json").toString()))) {

            // 0 - title, 1 - content, 2 - categories
            long[] totalDocLength = { 0, 0, 0 };

            String dataLine;
            while ((dataLine = dataReader.readLine()) != null) {
                try {
                    JSONObject dataJson = new JSONObject(dataLine);

                    int docId = dataJson.getInt("id");
                    String title = dataJson.getString("title").toLowerCase();
                    String content = dataJson.getString("content").toLowerCase();
                    String categories = dataJson.getJSONArray("categories").toList().stream()
                            .map(Objects::toString).map(String::toLowerCase)
                            .collect(Collectors.joining(" "));

                    totalDocLength[0] += putLength(docId, 0, title);
                    totalDocLength[1] += putLength(docId, 1, content);
                    totalDocLength[2] += putLength(docId, 2, categories);

                    ++indexedCount;
                    if (indexedCount % 1000 == 0) {
                        System.out.format("DocumentLengthImportThread has imported %d pages. Elapsed time: %s.%n",
                                indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
                    }
                } catch (Exception e) {
                    System.out.println("DocumentLengthImportThread throws an Exception.");
                    e.printStackTrace();
                }
            }

            for (int i = 0; i < totalDocLength.length; i++) {
                double averageDocLength = totalDocLength[i] / (double) indexedCount;
                database.put(JniDBFactory.bytes("__avgDocLength_" + i), JniDBFactory.bytes(String.valueOf(averageDocLength)));
                System.out.println("Summary: Average document length for field " + i + " is " + averageDocLength +
                        " (total: " + totalDocLength[i] + ").");
            }

            database.put(JniDBFactory.bytes("__docCount"), JniDBFactory.bytes(String.valueOf(indexedCount)));
            System.out.format("Summary: DocumentLengthImportThread has imported %d pages. Elapsed time: %s.%n",
                    indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
        } catch (FileNotFoundException e) {
            System.out.println("DocumentLengthImportThread throws a FileNotFoundException.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("DocumentLengthImportThread throws an IOException.");
            e.printStackTrace();
        }
    }
}
