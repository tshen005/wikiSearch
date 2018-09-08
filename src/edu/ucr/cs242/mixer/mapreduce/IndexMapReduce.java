package edu.ucr.cs242.mixer.mapreduce;

import edu.ucr.cs242.Utility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class IndexMapper extends Mapper<Object, Text, Text, IndexWritable> {
    private final SnowballStemmer stemmer = new englishStemmer();

    private void mapInvertedIndex(Map<String, List<Integer>> frequency,
                                  Map<String, List<List<Integer>>> position,
                                  int fieldCount, int fieldId, String value) {
        StringTokenizer tokenizer = new StringTokenizer(value);

        int tokenCount = 0;
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken()
                    // Remove the beginning and ending punctuation
                    .replaceAll("^\\p{Punct}*|\\p{Punct}*$", "")
                    // Ensure lower case
                    .trim().toLowerCase();

            // We only index alphanumeric and non-empty words
            if (Pattern.matches("^[\\p{Alnum}]+$", token)) {
                if (!Utility.isStopWord(token)) {
                    // Stemming through Snowball
                    stemmer.setCurrent(token);
                    stemmer.stem();
                    token = stemmer.getCurrent();

                    if (!frequency.containsKey(token)) {
                        frequency.put(token, new ArrayList<>(Collections.nCopies(fieldCount, 0)));
                        position.put(token, Stream.generate(ArrayList<Integer>::new).limit(fieldCount).collect(Collectors.toList()));
                    }

                    frequency.get(token).set(fieldId, frequency.get(token).get(fieldId) + 1);
                    position.get(token).get(fieldId).add(tokenCount);
                }
            }

            ++tokenCount;
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject json = new JSONObject(value.toString());

            int id = json.getInt("id");
            // We index in lowercase
            String title = json.getString("title").toLowerCase();
            String content = json.getString("content").toLowerCase();
            String categories = json.getJSONArray("categories").toList().stream()
                    .map(Objects::toString).map(String::toLowerCase)
                    .collect(Collectors.joining(" "));

            // <key, <fieldId:freq>>
            Map<String, List<Integer>> frequency = new HashMap<>();
            // <key, <fieldId:[pos]>>
            Map<String, List<List<Integer>>> position = new HashMap<>();

            mapInvertedIndex(frequency, position, 3, 0, title);
            mapInvertedIndex(frequency, position, 3, 1, content);
            mapInvertedIndex(frequency, position, 3, 2, categories);

            for (Map.Entry<String, List<Integer>> entry : frequency.entrySet()) {
                context.write(
                        new Text(entry.getKey()),
                        new IndexWritable(id, entry.getValue().stream().mapToInt(i -> i).toArray(),
                                position.get(entry.getKey()).stream().flatMap(List::stream).mapToInt(i -> i).toArray())
                );
            }
        } catch (JSONException e) {
            // The last line of input file (the empty line), will trigger this exception.
            // But maybe possible some other problem occurred
            if (!value.toString().isEmpty()) {
                System.out.println("JSONException, with value of `" + value.toString() + "`");
                e.printStackTrace();
            }
        }
    }
}

class IndexReducer extends Reducer<Text, IndexWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IndexWritable> values, Context context) throws IOException, InterruptedException {
        String value = StreamSupport.stream(values.spliterator(), false)
                .map(IndexWritable::toString).collect(Collectors.joining(";"));

        context.write(key, new Text(value));
    }
}

public class IndexMapReduce {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: mapreduce <json-data-input-path> <index-output-path>");
        } else {
            Job job = Job.getInstance(new Configuration(), "MapReduceIndexer");
            job.setJarByClass(IndexMapReduce.class);

            job.setMapperClass(IndexMapper.class);
            job.setReducerClass(IndexReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IndexWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
    }
}
