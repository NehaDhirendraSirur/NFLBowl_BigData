package org.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PY_reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalYards = 0;

        // Sum up all passing yards for a player
        for (IntWritable val : values) {
            totalYards += val.get();
        }

        result.set(totalYards);
        context.write(key, result);
    }
}

