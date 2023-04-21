package src;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    Text keyOut = new Text();
    Text valOut = new Text();
    
    public void map(LongWritable key, Text vals, Context con) throws IOException, InterruptedException {
        String line = vals.toString().trim();
        String[] words = line.split(",");

        keyOut.set(words[0]);
        valOut.set(words[1]+","+words[2]+","+words[3]);

        con.write(keyOut, valOut);
    } 
}
