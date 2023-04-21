package src;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class joinMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text val, Context con) throws IOException, InterruptedException {
        //
        String line = val.toString();
        String[] words = line.split(",");

        Text id = new Text(words[0]);
        Text data = new Text("Loc," + words[1] + " " + words[2]);

        con.write(id, data);
    }
}