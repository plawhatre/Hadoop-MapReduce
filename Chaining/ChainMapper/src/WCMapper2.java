package src;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper2 extends Mapper<Text, IntWritable, Text, IntWritable> {
    public void map(Text key, IntWritable value, Context con) throws IOException, InterruptedException {
        String keyOutput = key.toString().toUpperCase();
        con.write(new Text(keyOutput), value);
    }
}