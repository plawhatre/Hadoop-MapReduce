package src;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;


public class HikeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {


    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;
        for(DoubleWritable val: values) {
            sum += val.get();
            ++count; 
        }
        context.write(key, new DoubleWritable(sum/count));
    }
}