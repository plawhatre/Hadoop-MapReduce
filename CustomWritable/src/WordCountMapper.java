package src;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class WordCountMapper extends Mapper<LongWritable, Text, WCWritable, IntWritable>{
    
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
        String line = value.toString();
        String[] words = line.split(",");

        for (String word: words){
            WCWritable OutputKey = new WCWritable(word.toUpperCase().trim());
            IntWritable OutputValue = new IntWritable(1);

            con.write(OutputKey, OutputValue);
        }
    }
}