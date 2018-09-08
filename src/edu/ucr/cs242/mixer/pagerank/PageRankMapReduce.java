package edu.ucr.cs242.mixer.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

// Input: <PageN,RankN> -> <PageA, PageB, ...> (PageN's outlinks)
class PageRankMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] keyPair = key.toString().split(",");
        int docId = Integer.parseInt(keyPair[0]);
        double docRank = Double.parseDouble(keyPair[1]);

        System.out.println("<" + docId + ":" + docRank + ">: " + value.toString());

        // Is there any outlinks?
        if (!value.toString().isEmpty()) {
            List<Integer> outlinks = Arrays.stream(value.toString().split(","))
                    .map(Integer::parseInt).collect(Collectors.toList());

            System.out.println("<" + docId + ">: " + docRank / outlinks.size());

            for (Integer page : outlinks) {
                // PageK -> <PageN, RankN/NumOfPageNOutLinks>
                context.write(new Text(page.toString()),
                        new Text("i:" + docId + "," + docRank / outlinks.size()));
            }
        }

        // PageN -> <PageA, PageB, ...> (PageN's outlinks)
        context.write(new Text(String.valueOf(docId)), new Text("o:" + value.toString()));

        // PageN -> RankN (original page rank)
        context.write(new Text(String.valueOf(docId)), new Text("r:" + docRank));
    }
}

// Output: <PageN, RankN> -> <PageA, PageB, ...> (PageN's outlinks)
class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    private double dampingFactor;
    private long numOfDocs;
    private double convergenceScaleFactor;

    public enum Counter {
        CONVERGENCE
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        numOfDocs = conf.getLong("edu.ucr.cs242.mixer.pagerank.PageRankMapReduce.numberOfDocument", 0);
        dampingFactor = conf.getDouble("edu.ucr.cs242.mixer.pagerank.PageRankMapReduce.dampingFactor", 0.85);
        convergenceScaleFactor = conf.getDouble("edu.ucr.cs242.mixer.pagerank.PageRankMapReduce.convergenceScaleFactor", 1 / 1e-6);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double lastDocRank = 0;
        double docRank = (1 - dampingFactor) / numOfDocs;
        String outlinks = "";

        for (Text value : values) {
            String[] valuePair = value.toString().split(":");
            if (valuePair.length == 2) {
                // Inlinks with their PageRank?
                if (valuePair[0].equals("i")) {
                    // Is there any inlinks?
                    if (!valuePair[1].isEmpty()) {
                        String[] rankPair = valuePair[1].split(",");
                        int docId = Integer.parseInt(rankPair[0]);
                        double rankN = Double.parseDouble(rankPair[1]);
                        docRank += rankN * dampingFactor;
                    }
                } else if (valuePair[0].equals("o")) {
                    // Outlinks of this page
                    outlinks = valuePair[1];
                } else if (valuePair[0].equals("r")) {
                    lastDocRank = Double.parseDouble(valuePair[1]);
                }
            }
        }

        long scaledDelta = (long) (Math.abs(docRank - lastDocRank) * convergenceScaleFactor);
        context.getCounter(Counter.CONVERGENCE).increment(scaledDelta);

        System.out.println("<" + key.toString() + "," + lastDocRank + "," + docRank + "," + scaledDelta + "> : " + outlinks);
        context.write(new Text(key.toString() + "," + docRank), new Text(outlinks));
    }
}

public class PageRankMapReduce {
    private final String jsonLinkInputPath;
    private final String pageRankOutputPath;
    private final double dampingFactor;
    private final double convergence;

    /**
     * Construct a PageRank MapReducer, with given settings.
     * @param dampingFactor      The damping factor use in PageRank calculation.
     * @param convergence        The convergence limit (epsilon).
     * @param jsonLinkInputPath  The HDFS path to the SQLExporter's link.json.
     * @param pageRankOutputPath The HDFS path to the output.
     */
    public PageRankMapReduce(double dampingFactor, double convergence, String jsonLinkInputPath, String pageRankOutputPath) {
        this.dampingFactor = dampingFactor;
        this.convergence = convergence;
        this.jsonLinkInputPath = jsonLinkInputPath;
        this.pageRankOutputPath = pageRankOutputPath;
    }

    private long processInputFile(Configuration jobConf, Path originalInputFile, Path processedFile) throws IOException {
        FileSystem fs = originalInputFile.getFileSystem(jobConf);

        long numOfDocs = 0;
        Map<Integer, String> linkGraph = new HashMap<>();

        System.out.println("Processing input file...");
        try (DataInputStream inputStream = fs.open(originalInputFile);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    JSONObject json = new JSONObject(line);

                    int id = json.getInt("id");
                    String links = json.getJSONArray("links").toList().stream()
                            .map(Objects::toString) // actually they are integers
                            .collect(Collectors.joining(","));

                    linkGraph.put(id, links);
                    ++numOfDocs;
                } catch (JSONException e) {
                    // The last line of input file (the empty line), will trigger this exception.
                    // But maybe possible some other problem occurred
                    if (!line.isEmpty()) {
                        System.out.println("JSONException, with value of `" + line + "`");
                        e.printStackTrace();
                    }
                }
            }
        }

        System.out.println("There are totally " + numOfDocs + " documents.");

        System.out.println("Writing to " + processedFile.toString() + "...");
        double initialPageRank = 1.0 / (double) numOfDocs;
        try (DataOutputStream outputStream = fs.create(processedFile);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {

            long linesWritten = 0;
            for (Map.Entry<Integer, String> entry : linkGraph.entrySet()) {
                writer.write(entry.getKey() + "," + initialPageRank + "\t" + entry.getValue() + "\n");
                ++linesWritten;

                if (linesWritten % 10000 == 0 || linesWritten == numOfDocs) {
                    System.out.println(linesWritten + " lines have been written.");
                }
            }
        }

        return numOfDocs;
    }

    private double runPageRankJob(Configuration jobConf, int iteration, Path inputPath, Path outputPath) throws Exception {
        Job job = Job.getInstance(jobConf, "PageRank-Iteration-" + iteration);
        job.setJarByClass(PageRankMapReduce.class);

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new Exception("Job PageRank-Iteration-" + iteration + " failed.");
        }

        long scaledConvergence = job.getCounters().findCounter(PageRankReducer.Counter.CONVERGENCE).getValue();
        double retConv = (double) scaledConvergence * convergence;
        System.out.println("Iteration: " + iteration + ", scaledConvergence = " + scaledConvergence +
                ", convergence = " + retConv + ".");
        return retConv;
    }

    public void start() throws Exception {
        Configuration jobConf = new Configuration();
        jobConf.setDouble("edu.ucr.cs242.mixer.pagerank.PageRankMapReduce.dampingFactor", dampingFactor);
        jobConf.setDouble("edu.ucr.cs242.mixer.pagerank.PageRankMapReduce.convergenceScaleFactor", 1 / convergence);

        Path outputPath = new Path(pageRankOutputPath);
        outputPath.getFileSystem(jobConf).delete(outputPath, true);
        outputPath.getFileSystem(jobConf).mkdirs(outputPath);

        Path inputPath = new Path(pageRankOutputPath, "link-input");
        long numOfDocs = processInputFile(jobConf, new Path(jsonLinkInputPath), inputPath);
        jobConf.setLong("edu.ucr.cs242.mixer.pagerank.PageRankMapReduce.numberOfDocument", numOfDocs);

        for (int iter = 1; ; iter++) {
            Path jobOutputPath = new Path(outputPath, "iteration-" + iter);

            System.out.println("Iteration: " + iter +", output to " + jobOutputPath.toString() + ".");
            if (runPageRankJob(jobConf, iter, inputPath, jobOutputPath) < convergence) {
                System.out.println("Converged! PageRank has been computed.");
                break;
            }

            inputPath = jobOutputPath;
        }
    }

    public static void main(String[] args) throws Exception {
        final double DAMPING_FACTOR = 0.85;
        final double CONVERGENCE = 1e-6;

        if (args.length < 2 || args.length > 4) {
            System.out.println("usage: pagerank <json-link-input-path> <pagerank-output-path> [damping-factor] [convergence]");
        } else {
            try {
                double dampingFactor = DAMPING_FACTOR;
                double convergence = CONVERGENCE;

                if (args.length >= 3) {
                    dampingFactor = Double.parseDouble(args[2]);
                }

                if (args.length >= 4) {
                    convergence = Double.parseDouble(args[3]);
                }

                new PageRankMapReduce(dampingFactor, convergence, args[0], args[1]).start();
            } catch (NumberFormatException e) {
                System.out.println("pagerank: invalid option(s)");
                System.exit(1);
            }

        }
    }
}
