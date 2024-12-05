package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PY_runner {
    public static void main(String[] args) throws Exception {
        // Check if input and output paths are provided
        if (args.length != 2) {
            System.err.println("Usage: PenaltyYardsRunner <input path> <output path>");
            System.exit(-1);
        }

        // Create a new configuration and job instance
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Penalty Yards Sum");

        // Set the job's jar file and main classes
        job.setJarByClass(PY_runner.class);
        job.setMapperClass(PY_mapper.class);
        job.setReducerClass(PY_reducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        FileOutputFormat.setOutputPath(job,  new Path(args[args.length - 1]));

        // Exit after job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

