package edu.ucr.cs242.mixer.importer;

import edu.ucr.cs242.Utility;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;

import java.io.*;
import java.time.LocalDateTime;

public class PageRankImportThread extends Thread {
    private final DB database;
    private final String hadoopPageRankOutputPath;

    /**
     * Construct a page rank import thread, with given settings.
     * @param database                 The LevelDB object.
     * @param hadoopPageRankOutputPath The file name to the Hadoop's PageRank output.
     */
    public PageRankImportThread(DB database, String hadoopPageRankOutputPath) {
        this.database = database;
        this.hadoopPageRankOutputPath = hadoopPageRankOutputPath;
    }

    @Override
    public void run() {
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("PageRankImportThread started at " + startAt.toLocalTime() + ".");

        int indexedCount = 0;
        double maxPageRank = 0;
        try (BufferedReader dataReader = new BufferedReader(new FileReader(new File(hadoopPageRankOutputPath)))) {

            String dataLine;
            while ((dataLine = dataReader.readLine()) != null) {
                try {
                    String[] keyPair = dataLine.split("\t")[0].split(",");
                    int docId = Integer.parseInt(keyPair[0]);
                    double docRank = Double.parseDouble(keyPair[1]);
                    maxPageRank = Math.max(docRank, maxPageRank);

                    // <docId, docRank>
                    database.put(JniDBFactory.bytes("__docPR_" + docId), JniDBFactory.bytes(String.valueOf(docRank)));

                    ++indexedCount;
                    if (indexedCount % 1000 == 0) {
                        System.out.format("PageRankImportThread has imported %d pages. Elapsed time: %s.%n",
                                indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
                    }
                } catch (Exception e) {
                    System.out.println("PageRankImportThread throws an Exception.");
                    e.printStackTrace();
                }
            }

            database.put(JniDBFactory.bytes("__docMaxPR"), JniDBFactory.bytes(String.valueOf(maxPageRank)));
            System.out.format("Summary: PageRankImportThread has imported %d pages. Elapsed time: %s.%n",
                    indexedCount, Utility.elapsedTime(startAt, LocalDateTime.now()));
            System.out.println("Summary: The max PageRank is " + maxPageRank + ".");
        } catch (FileNotFoundException e) {
            System.out.println("PageRankImportThread throws a FileNotFoundException.");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("PageRankImportThread throws an IOException.");
            e.printStackTrace();
        }
    }
}
