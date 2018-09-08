package edu.ucr.cs242;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class Launcher {
    private static Map<String, Subroutine> subroutines = new LinkedHashMap<>();

    static {
        subroutines.put("crawler",
                new Subroutine("crawler",
                        "edu.ucr.cs242.crawler.WikiCrawler",
                        "execute the Wikipedia crawler"));
        subroutines.put("indexer",
                new Subroutine("indexer",
                        "edu.ucr.cs242.indexing.IndexMapReduce",
                        "execute the Lucene indexer"));
        subroutines.put("exporter",
                new Subroutine("exporter",
                        "edu.ucr.cs242.mixer.exporter.SQLExporter",
                        "export the data in SQLite into JSON file"));
        subroutines.put("mapreduce",
                new Subroutine("mapreduce",
                        "edu.ucr.cs242.mixer.mapreduce.IndexMapReduce",
                        "execute the Mixer MapReduce indexer"));
        subroutines.put("pagerank",
                new Subroutine("pagerank",
                        "edu.ucr.cs242.mixer.pagerank.PageRankMapReduce",
                        "execute the Mixer PageRank MapReduce"));
        subroutines.put("importer",
                new Subroutine("importer",
                        "edu.ucr.cs242.mixer.importer.NoSQLImporter",
                        "import the hadoop output into NoSQL database"));
        subroutines.put("webapi",
                new Subroutine("webapi",
                        "edu.ucr.cs242.webapi.WebAPI",
                        "execute the RESTful API server"));
    }

    private static void printMessage(String message) {
        System.out.println("cs242: " + message);
    }

    private static void printUsage() {
        System.out.println("usage: cs242 <subroutine> [options] <arguments...>");
        System.out.println("possible subroutines:");
        subroutines.forEach((key, value) -> System.out.format(" %-10s%s%n", key, value.getDescription()));
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        if (args.length <= 0) {
            printMessage("subroutine is not specified");
            printUsage();
        }

        if (!subroutines.containsKey(args[0])) {
            printMessage("invalid subroutine: " + args[0]);
            printUsage();
        }

        Class<?> clazz = Class.forName(subroutines.get(args[0]).getClassName());
        Method method = clazz.getMethod("main", String[].class);
        method.invoke(null, (Object) Arrays.stream(args).skip(1).toArray(String[]::new));
    }
}
