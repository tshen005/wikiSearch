package edu.ucr.cs242.mixer.importer;

import edu.ucr.cs242.Utility;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class NoSQLImporter {
    private final String databasePath;
    private final String jsonOutputPath;
    private final String hadoopIndexOutputPath;
    private final String hadoopPageRankOutputPath;

    /**
     * Construct an NoSQLImporter with given settings.
     * @param databasePath          The path to LevelDB database.
     * @param jsonOutputPath        The folder to the JSON output.
     * @param hadoopIndexOutputPath The file name to the Hadoop's index output.
     * @param hadoopPageRankOutputPath The file name to the Hadoop's PageRank output.
     */
    public NoSQLImporter(String databasePath, String jsonOutputPath, String hadoopIndexOutputPath, String hadoopPageRankOutputPath) {
        this.databasePath = databasePath;
        this.jsonOutputPath = jsonOutputPath;
        this.hadoopIndexOutputPath = hadoopIndexOutputPath;
        this.hadoopPageRankOutputPath = hadoopPageRankOutputPath;
    }

    public void start() throws IOException {
        org.iq80.leveldb.Options options = new org.iq80.leveldb.Options();
        options.createIfMissing(true);

        try (DB db = JniDBFactory.factory.open(new File(databasePath), options)) {
            Thread indexThread = new IndexImportThread(db, jsonOutputPath);
            indexThread.start();

            Thread dataThread = new DataImportThread(db, hadoopIndexOutputPath);
            dataThread.start();

            DocumentLengthImportThread lengthThread = new DocumentLengthImportThread(db, jsonOutputPath);
            lengthThread.start();

            PageRankImportThread pageRankThread = new PageRankImportThread(db, hadoopPageRankOutputPath);
            pageRankThread.start();

            Utility.waitThread(indexThread);
            Utility.waitThread(dataThread);
            Utility.waitThread(lengthThread);
            Utility.waitThread(pageRankThread);
        }
    }

    private static void printMessage(String message) {
        System.out.println("importer: " + message);
    }

    private static void printUsage() {
        System.out.println("usage: importer [options] <leveldb-path> <exporter-json-output-path> <hadoop-index-output-path> <hadoop-pagerank-output-path>");
        System.out.println("use -h for a list of possible options");
        System.exit(1);
    }

    private static void printHelp(org.apache.commons.cli.Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("importer [options] <leveldb-path> <exporter-json-output-path> <hadoop-index-output-path> <hadoop-pagerank-output-path>", options);
        System.out.println();
    }

    public static void main(String[] args) throws IOException {
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption(org.apache.commons.cli.Option.builder("l")
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
                printMessage("LevelDB path is not specified");
                printUsage();
            }

            if (argList.size() <= 1) {
                printMessage("SQLExporter's JSON output path is not specified");
                printUsage();
            }

            if (argList.size() <= 2) {
                printMessage("Hadoop's index output path is not specified");
                printUsage();
            }

            if (argList.size() <= 3) {
                printMessage("Hadoop's PageRank output path is not specified");
                printUsage();
            }

            String logOutput = cmd.getOptionValue("log-output");
            if (!Utility.openOutputLog(logOutput)) {
                printMessage("invalid log file path");
                printUsage();
            }

            Path databasePath = Paths.get(argList.get(0));
            if (Files.exists(databasePath) &&  !Files.isDirectory(databasePath)) {
                printMessage("invalid LevelDB path (not directory)");
                printUsage();
            }

            Path jsonOutputPath = Paths.get(argList.get(1));
            if (!Files.exists(jsonOutputPath) || !Files.isDirectory(jsonOutputPath)) {
                printMessage("invalid SQLExporter's JSON output path (not exist or not directory)");
                printUsage();
            }

            Path hadoopIndexOutputPath = Paths.get(argList.get(2));
            if (!Files.exists(hadoopIndexOutputPath) || Files.isDirectory(hadoopIndexOutputPath)) {
                printMessage("invalid Hadoop's index output path (not exist or is directory)");
                printUsage();
            }

            Path hadoopPageRankOutputPath = Paths.get(argList.get(3));
            if (!Files.exists(hadoopPageRankOutputPath) || Files.isDirectory(hadoopPageRankOutputPath)) {
                printMessage("invalid Hadoop's PageRank output path (not exist or is directory)");
                printUsage();
            }

            new NoSQLImporter(databasePath.toString(), jsonOutputPath.toString(),
                    hadoopIndexOutputPath.toString(), hadoopPageRankOutputPath.toString()).start();
        } catch (ParseException e) {
            // Lower the first letter, which as default is an upper letter.
            printMessage(e.getMessage().substring(0, 1).toLowerCase() + e.getMessage().substring(1));
            printHelp(options);
            System.exit(1);
        }
    }
}
