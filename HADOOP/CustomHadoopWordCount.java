package com.mapreduce.wc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class CustomHadoopWordCount {

    // Custom implementation of MapReduce logic using Hadoop's filesystem
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Ensure correct input arguments
        if (files.length < 2) {
            System.err.println("Usage: CustomHadoopWordCount <input path> <output path>");
            System.exit(-1);
        }

        Path inputPath = new Path(files[0]);
        Path outputPath = new Path(files[1]);
        Path tempPath = new Path(files[1] + "_temp");

        FileSystem fs = FileSystem.get(conf);

        // Check if output directory exists, delete if it does
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // Create output directory
        fs.mkdirs(outputPath);

        // Check if temp directory exists, delete if it does
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }

        // Create temp directory for mapper output
        fs.mkdirs(tempPath);

        System.out.println("Starting custom MapReduce job...");

        // Step 1: Map Phase
        System.out.println("Starting Map phase...");
        List<Path> mapOutputs = runMapPhase(fs, conf, inputPath, tempPath);

        // Step 2: Reduce Phase
        System.out.println("Starting Reduce phase...");
        runReducePhase(fs, conf, mapOutputs, outputPath);

        // Clean up temporary files
        fs.delete(tempPath, true);

        System.out.println("Job completed successfully");
    }

    private static List<Path> runMapPhase(FileSystem fs, Configuration conf,
                                         Path inputPath, Path tempPath) throws IOException {
        List<Path> mapOutputs = new ArrayList<>();

        // Get all input files
        FileStatus[] inputFiles = fs.listStatus(inputPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return !path.getName().startsWith("_") && !path.getName().startsWith(".");
            }
        });

        int mapTaskId = 0;

        for (FileStatus fileStatus : inputFiles) {
            Path file = fileStatus.getPath();

            // Skip directories
            if (fs.getFileStatus(file).isDirectory()) {
                continue;
            }

            Path mapOutputFile = new Path(tempPath, "map_output_" + mapTaskId);
            mapOutputs.add(mapOutputFile);

            System.out.println("Processing input file: " + file.getName());

            try (InputStream in = fs.open(file);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                 OutputStream out = fs.create(mapOutputFile);
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    // Map function: Split line into words
                    String[] words = line.trim().split("\\s+");

                    for (String word : words) {
                        if (!word.isEmpty()) {
                            // Emit (word, 1) pairs
                            writer.write(word.trim().toUpperCase() + "\t1");
                            writer.newLine();
                        }
                    }
                }
            }

            mapTaskId++;
        }

        return mapOutputs;
    }

    private static void runReducePhase(FileSystem fs, Configuration conf,
                                      List<Path> mapOutputs, Path outputPath) throws IOException {
        // Step 1: Read and combine all mapper outputs
        Map<String, Integer> wordCounts = new HashMap<>();

        for (Path mapOutput : mapOutputs) {
            try (InputStream in = fs.open(mapOutput);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        String word = parts[0];
                        int count = Integer.parseInt(parts[1]);

                        // Aggregate counts for the same word
                        wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
                    }
                }
            }
        }

        // Sort the words (optional, for consistent output ordering)
        List<String> sortedWords = new ArrayList<>(wordCounts.keySet());
        Collections.sort(sortedWords);

        // Step 2: Write the reduced output
        Path finalOutputFile = new Path(outputPath, "part-r-00000");

        try (OutputStream out = fs.create(finalOutputFile);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {

            for (String word : sortedWords) {
                // Write (word, count) pairs
                writer.write(word + "\t" + wordCounts.get(word));
                writer.newLine();
            }
        }
    }
}
