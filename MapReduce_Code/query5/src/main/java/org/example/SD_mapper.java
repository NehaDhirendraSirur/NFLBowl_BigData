package org.example;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SD_mapper extends Mapper<Object, Text, Text, FloatWritable> {
    private Text playerName = new Text();
    private FloatWritable passingYards = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println(line);
        String[] fields = line.split(","); // Assuming CSV format

        // Extract relevant fields
        String displayName = fields[3]; // Assuming 2nd column is player name
        int hadDropped = Integer.parseInt(fields[7]); // Assuming 4th column is hadDropped
        int sumOfPassingYards = Integer.parseInt(fields[58]); // Assuming 5th column is sum of passing yards

        // Process only rows where hadDropped is 1
        if (hadDropped >0) {
            playerName.set(displayName);
            passingYards.set(sumOfPassingYards);
            context.write(playerName, passingYards);
        }
    }
}