package edu.ucr.cs242.mixer.exporter;

import edu.ucr.cs242.Utility;
import org.apache.commons.cli.*;
import org.json.JSONObject;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SQLExporter {
    /**
     * The number of records to be batch-read per SQL transaction.
     */
    public static final int BATCH_READ_COUNT = 50;

    /**
     * The SQL query statement.
     */
    public static final String SQL_QUERY = "SELECT title, content, categories, outLinks FROM pages LIMIT ? OFFSET ?";

    private final Connection dbConnection;
    private final String jsonOutputPath;
    private final int numOfPages;

    /**
     * Construct an SQLExporter with given settings.
     * @param dbConnection   The active database connection.
     * @param jsonOutputPath The folder to output JSON format data.
     */
    public SQLExporter(Connection dbConnection, String jsonOutputPath) {
        this.dbConnection = dbConnection;
        this.jsonOutputPath = jsonOutputPath;

        numOfPages = Utility.fetchPageCount(dbConnection);
        // Check number of pages we have.
        if (numOfPages <= 0) {
            System.out.println("SQLExporter cannot find any pages to export. Exiting...");
            System.exit(numOfPages);
        }
    }

    private void reportProgress(LocalDateTime startAt, int count, boolean links) {
        if (count == numOfPages || count % 1000 == 0) {
            System.out.format("%sSQLExporter has exported %d pages%s, %.2f%% completed. Elapsed time: %s.%n",
                    count == numOfPages ? "Summary: " : "",
                    count,
                    links ? "' outgoing links" : "",
                    count * 100.0f / numOfPages, Utility.elapsedTime(startAt, LocalDateTime.now()));
        }
    }

    public void start() {
        LocalDateTime startAt = LocalDateTime.now();
        System.out.println("SQLExporter started at " + startAt.toLocalTime() + ". " +
                "Pages to export: " + numOfPages + ".");

        Map<String, Integer> titleToId = new HashMap<>();
        Map<String, List<String>> outgoingLinks = new HashMap<>();

        try {
            int writtenCount = 0;
            FileOutputStream dataOutput = new FileOutputStream(Paths.get(jsonOutputPath, "data.json").toString());
            FileOutputStream indexOutput = new FileOutputStream(Paths.get(jsonOutputPath, "index.json").toString());
            FileOutputStream linkOutput = new FileOutputStream(Paths.get(jsonOutputPath, "link.json").toString());

            while (writtenCount < numOfPages) {
                try (PreparedStatement statement = dbConnection.prepareStatement(SQL_QUERY)) {
                    statement.setInt(1, Math.min(BATCH_READ_COUNT, numOfPages - writtenCount));
                    statement.setInt(2, writtenCount);

                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            String title = result.getString("title");
                            String content = result.getString("content");
                            List<String> categories =
                                    Arrays.stream(result.getString("categories").split(Pattern.quote("|")))
                                            .collect(Collectors.toList());
                            List<String> outLinks =
                                    Arrays.stream(result.getString("outLinks").split(Pattern.quote("|")))
                                            .collect(Collectors.toList());

                            JSONObject object = new JSONObject()
                                    .put("id", writtenCount)
                                    .put("title", title)
                                    .put("content", content)
                                    .put("categories", categories);

                            dataOutput.write(object.toString().getBytes("utf-8"));
                            dataOutput.write('\n');
                            dataOutput.flush();

                            object = new JSONObject()
                                    .put("id", writtenCount)
                                    .put("title", title);

                            indexOutput.write(object.toString().getBytes("utf-8"));
                            indexOutput.write('\n');
                            indexOutput.flush();

                            titleToId.put(title, writtenCount);
                            outgoingLinks.put(title, outLinks);

                            ++writtenCount;
                        }
                    }

                    reportProgress(startAt, writtenCount, false);
                } catch (SQLException e) {
                    System.out.println("SQLExporter throws an SQLException.");
                    e.printStackTrace();
                }
            }

            writtenCount = 0;
            for (Map.Entry<String, Integer> entry : titleToId.entrySet()) {
                List<Integer> links = outgoingLinks.get(entry.getKey()).stream()
                        .map(titleToId::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                if (!links.isEmpty()) {
                    JSONObject object = new JSONObject()
                            .put("id", entry.getValue())
                            .put("links", links);

                    linkOutput.write(object.toString().getBytes("utf-8"));
                    linkOutput.write('\n');
                    linkOutput.flush();
                }

                ++writtenCount;
                reportProgress(startAt, writtenCount, true);
            }

            dataOutput.close();
            indexOutput.close();
            linkOutput.close();
        } catch (IOException e) {
            System.out.println("SQLExporter throws an IOException: " + e.getMessage());
        }
    }

    private static void printMessage(String message) {
        System.out.println("exporter: " + message);
    }

    private static void printUsage() {
        System.out.println("usage: exporter [options] <jdbc-url> <json-output-path>");
        System.out.println("use -h for a list of possible options");
        System.exit(1);
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("exporter [options] <jdbc-url> <json-output-path>", options);
        System.out.println();
    }

    public static void main(String args[]) throws SQLException, ClassNotFoundException {
        Options options = new Options();
        options.addOption(Option.builder("l")
                .longOpt("log-output")
                .argName("FILE NAME")
                .desc("the file to write logs into (default: STDOUT)")
                .numberOfArgs(1)
                .build());

        options.addOption("h", "help", false, "print a synopsis of standard options");

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);
            List<String> argList = cmd.getArgList();

            if (cmd.hasOption("h")) {
                printHelp(options);
                System.exit(0);
            }

            if (argList.isEmpty()) {
                printMessage("JDBC url is not specified");
                printUsage();
            }

            if (argList.size() <= 1) {
                printMessage("JSON output path is not specified");
                printUsage();
            }

            String logOutput = cmd.getOptionValue("log-output");
            if (!Utility.openOutputLog(logOutput)) {
                printMessage("invalid log file path");
                printUsage();
            }

            Optional<Connection> dbConnection = Utility.getConnection(argList.get(0));
            if (!dbConnection.isPresent()) {
                printMessage("invalid JDBC url");
                printUsage();
            } else {
                Path jsonOutputPath = Paths.get(argList.get(1));
                if (!Files.exists(jsonOutputPath) || !Files.isDirectory(jsonOutputPath)) {
                    printMessage("invalid JSON output path (not exist or not directory)");
                    printUsage();
                }

                new SQLExporter(dbConnection.get(), jsonOutputPath.toString()).start();
                dbConnection.get().close();
            }
        } catch (ParseException e) {
            // Lower the first letter, which as default is an upper letter.
            printMessage(e.getMessage().substring(0, 1).toLowerCase() + e.getMessage().substring(1));
            printHelp(options);
            System.exit(1);
        }
    }
}
