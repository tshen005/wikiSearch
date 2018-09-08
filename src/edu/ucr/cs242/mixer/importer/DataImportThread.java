package edu.ucr.cs242.mixer.importer;

import edu.ucr.cs242.Utility;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DataImportThread extends Thread {
    private final DB database;
    private final String hadoopIndexOutputPath;

    /**
     * Construct a data import thread, with given settings.
     * @param database              The LevelDB object.
     * @param hadoopIndexOutputPath The file name to the Hadoop's index output.
     */
    public DataImportThread(DB database, String hadoopIndexOutputPath) {
        this.database = database;
        this.hadoopIndexOutputPath = hadoopIndexOutputPath;
    }

    private void processDataLine(String dataLine) {
        String[] data = dataLine.split("\t");
        String keyword = data[0];

        JSONArray value = new JSONArray();
        Arrays.stream(data[1].split(";")).forEach(compact -> {
            String[] index = compact.split(":");
            String docId = index[0];

            String[] freqPos = index[1].split(Pattern.quote("|"));
            List<Integer> frequency = Arrays.stream(freqPos[0].split(","))
                    .map(Integer::parseInt).collect(Collectors.toList());

            String[] pos = freqPos[1].split(",");
            List<List<Integer>> position = new ArrayList<>();

            for (int count = 0, i = 0; i < frequency.size(); i++) {
                int freq = frequency.get(i);
                position.add(Arrays.stream(pos).skip(count).limit(freq)
                        .map(Integer::parseInt).collect(Collectors.toList()));
                count += freq;
            }

            value.put(new JSONObject()
                    .put(docId, new JSONObject()
                            .put("frequency", frequency)
                            .put("position", position)
                    ));
        });

        database.put(JniDBFactory.bytes(keyword), JniDBFactory.bytes(value.toString()));
    }

    @Override
    public void run() {
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("DataImportThread started at " + startAt.toLocalTime() + ".");

        int indexedCount = 0;

        try (BufferedReader dataReader = new BufferedReader(new FileReader(new File(hadoopIndexOutputPath)))) {

            String dataLine;
            while ((dataLine = dataReader.readLine()) != null) {
                try {
                    processDataLine(dataLine);

                    ++indexedCount;
                    if (indexedCount % 1000 == 0) {
                        System.out.format("DataImportThread has imported %d keywords. Elapsed time: %s.%n",
                                indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
                    }
                } catch (Exception e) {
                    System.out.println("DataImportThread throws an Exception.");
                    e.printStackTrace();
                }
            }

            System.out.format("Summary: DataImportThread has imported %d keywords. Elapsed time: %s.%n",
                    indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
        } catch (FileNotFoundException e) {
            System.out.println("DataImportThread throws a FileNotFoundException.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("DataImportThread throws an IOException.");
            e.printStackTrace();
        }
    }
}
