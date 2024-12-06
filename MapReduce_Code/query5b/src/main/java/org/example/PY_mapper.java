package org.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PY_mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text playerName = new Text();
    private IntWritable passingYards = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println(line);
        String[] fields = line.split(","); // Assuming CSV format

        // Extract relevant fields
        String displayName = fields[3]; // Assuming 2nd column is player name
        // Assuming 4th column is hadDropped
        int sumOfPassingYards = Integer.parseInt(fields[7]); // Assuming 5th column is sum of passing yards

        // Process only rows where hadDropped is 1
        if (sumOfPassingYards >0) {
            playerName.set(displayName);
            passingYards.set(sumOfPassingYards);
            context.write(playerName, passingYards);
        }
    }
}

