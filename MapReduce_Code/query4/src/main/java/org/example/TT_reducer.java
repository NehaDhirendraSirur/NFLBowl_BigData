package org.example;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TT_reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();
    //private static final float DIVISOR = 31100f; // Use a constant for flexibility

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float totalYards = 0;
        int count=0;

        // Sum up all passing yards for a player
        for (FloatWritable val : values) {
            totalYards += val.get();
            count++;
        }

        // Avoid division by zero
        if(count!=0) {
            result.set(totalYards/count);
            context.write(key, result);
        }else{
            System.out.println("Cant divide by Zero");
        }

    }
}
