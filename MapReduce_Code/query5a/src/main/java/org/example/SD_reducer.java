package org.example;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

public class SD_reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalYards = 0;
        int count=0;
        // Sum up all passing yards for a player
        for (IntWritable val : values) {
            totalYards += val.get();
            count++;
        }

        result.set(totalYards/count);
        context.write(key, result);
    }
}