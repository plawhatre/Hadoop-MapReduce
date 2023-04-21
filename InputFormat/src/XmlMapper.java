package src;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class XmlMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable key, Text vals, Context context) throws IOException, InterruptedException {
        // identity mapper
        context.write(key, vals);
    }
}