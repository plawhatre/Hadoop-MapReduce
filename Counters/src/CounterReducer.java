package src;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CounterReducer extends Reducer<Text, Text, Text, IntWritable> {
    public void reduce(Text key, Iterable<Text> vals, Context con) throws IOException, InterruptedException {
        int totalRev = 0;

        Iterator<Text> itr = vals.iterator();
        while (itr.hasNext()) {
            String[] data = itr.next().toString().split(",");
            int price = Integer.parseInt(data[0].trim());
            int sales = Integer.parseInt(data[1].trim());

            totalRev += price * sales;
        }

        con.write(key, new IntWritable(totalRev));
    }
}