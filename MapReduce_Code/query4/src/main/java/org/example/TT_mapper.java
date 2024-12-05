package org.example;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TT_mapper extends Mapper<Object, Text, Text, FloatWritable> {
    private Text playerName = new Text();
    private FloatWritable passingYards = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(","); // Assuming CSV format

        try {
            // Ensure there are enough fields
            if (fields.length > 29) {
                // Extract relevant fields
                String displayName = fields[6]; // Assuming 7th column is player name
                float sumOfPassingYards = Float.parseFloat(fields[29]); // Assuming 30th column is sum of passing yards

                // Set and write to context
                playerName.set(displayName);
                passingYards.set(sumOfPassingYards);
                context.write(playerName, passingYards);
            } else {
                System.err.println("Invalid row (not enough fields): " + line);
            }
        } catch (NumberFormatException e) {
            System.err.println("Invalid number format for passing yards: " + fields[29]);
        }
    }
}
