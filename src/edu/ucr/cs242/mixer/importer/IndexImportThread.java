package edu.ucr.cs242.mixer.importer;

import edu.ucr.cs242.Utility;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Paths;
import java.time.LocalDateTime;

public class IndexImportThread extends Thread {
    private final DB database;
    private final String jsonOutputPath;

    /**
     * Construct a index import thread, with given settings.
     * @param database       The LevelDB object.
     * @param jsonOutputPath The folder to the JSON output.
     */
    public IndexImportThread(DB database, String jsonOutputPath) {
        this.database = database;
        this.jsonOutputPath = jsonOutputPath;
    }

    @Override
    public void run() {
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("IndexImportThread started at " + startAt.toLocalTime() + ".");

        int indexedCount = 0;

        try (BufferedReader indexReader = new BufferedReader(
                new FileReader(Paths.get(jsonOutputPath, "index.json").toString()))) {

            String indexLine;
            while ((indexLine = indexReader.readLine()) != null) {
                try {
                    JSONObject indexJson = new JSONObject(indexLine);

                    // <docId,title>
                    database.put(JniDBFactory.bytes("__docId_" + indexJson.getInt("id")),
                            JniDBFactory.bytes(indexJson.getString("title")));

                    ++indexedCount;
                    if (indexedCount % 1000 == 0) {
                        System.out.format("IndexImportThread has imported %d pages. Elapsed time: %s.%n",
                                indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
                    }
                } catch (Exception e) {
                    System.out.println("IndexImportThread throws an Exception.");
                    e.printStackTrace();
                }
            }

            System.out.format("Summary: IndexImportThread has imported %d pages. Elapsed time: %s.%n",
                    indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
        } catch (FileNotFoundException e) {
            System.out.println("IndexImportThread throws a FileNotFoundException.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IndexImportThread throws an IOException.");
            e.printStackTrace();
        }
    }
}
