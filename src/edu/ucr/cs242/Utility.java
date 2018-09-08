package edu.ucr.cs242;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Utility {
    public static void waitThread(Thread thread) {
        if (thread != null) {
            try { thread.join(); }
            catch (InterruptedException e) { thread.interrupt(); }
        }
    }

    public static void waitThreads(Thread[] threads) {
        for (Thread thread : threads) {
            if (thread != null) {
                // Wait threads to exit.
                try { thread.join(); }
                catch (InterruptedException e) { thread.interrupt(); }
            }
        }
    }

    public static int calculatePartition(int numOfPages, int numOfThreads, int threadId) {
        int tasksPerThread = numOfPages / numOfThreads;
        tasksPerThread += threadId < (numOfPages % numOfThreads) ? 1 : 0;
        return tasksPerThread;
    }

    public static String elapsedTime(LocalDateTime start, LocalDateTime end) {
        Duration elapsed = Duration.between(start, end);

        long hours = elapsed.toHours();
        long minutes = elapsed.toMinutes() % 60;
        long seconds = elapsed.getSeconds() % 60;
        long milliseconds = elapsed.toMillis() % 1000;

        return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, milliseconds);
    }

    public static boolean openOutputLog(String logOutput) {
        if (logOutput != null) {
            try {
                PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(logOutput)), true);
                System.setOut(ps);
                return true;
            } catch (FileNotFoundException e) {
                return false;
            }
        }

        return true;
    }

    public static Optional<Connection> getConnection(String jdbcUrl) throws ClassNotFoundException {
        final String SQL_COUNT = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'pages'";
        Connection dbConnection = null;

        Class.forName("org.sqlite.JDBC");
        try {
            dbConnection = DriverManager.getConnection(jdbcUrl);
            Statement query = dbConnection.createStatement();
            ResultSet result = query.executeQuery(SQL_COUNT);

            result.next();
            int count = result.getInt(1);
            result.close();
            query.close();

            if (count > 0) {
                return Optional.of(dbConnection);
            } else {
                dbConnection.close();
                return Optional.empty();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    public static int fetchPageCount(Connection dbConnection) {
        final String SQL_COUNT = "SELECT COUNT(*) FROM pages";
        int numOfPages = -1;

        try (Statement query = dbConnection.createStatement();
             ResultSet result = query.executeQuery(SQL_COUNT)) {

            result.next();
            numOfPages = result.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return numOfPages;
    }

    private static final List<String> STOP_WORDS = Arrays.asList(
            "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
            "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",
            "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
            "having", "do", "does", "did", "doing", "would", "should", "could", "ought", "cannot", "a", "an",
            "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with",
            "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to",
            "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once",
            "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most",
            "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very"
    );

    public static boolean isStopWord(String word) {
        return STOP_WORDS.contains(word);
    }

    public static List<String> splitKeyword(String keyword) {
        // Split terms by space
        return Arrays.stream(keyword.split(" "))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    public static String levelDBGet(DB db, String key) {
        return JniDBFactory.asString(db.get(JniDBFactory.bytes(key)));
    }
}
