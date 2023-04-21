package src;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.IntWritable;

public class WordCountReducer extends Reducer<WCWritable, IntWritable, WCWritable, IntWritable>{
    public void reduce(WCWritable word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
        int sum = 0;
        
        for (IntWritable value: values){
            sum += value.get();
        }

        con.write(word, new IntWritable(sum));
    }
}
