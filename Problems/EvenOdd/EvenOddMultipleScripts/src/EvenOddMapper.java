package src;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class EvenOddMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text values, Context con) throws IOException, InterruptedException
        {
            String line = values.toString();
            String[] numbers = line.split(",");
            
            for (String number: numbers)
            {
                int new_number = Integer.parseInt(number);
                

                if (new_number % 2 == 0)
                {
                    Text OutputKey = new Text("Even");
                    IntWritable OutputValue =  new IntWritable(new_number);
                    con.write(OutputKey, OutputValue);
                } else
                {
                    Text OutputKey = new Text("Odd");
                    IntWritable OutputValue =  new IntWritable(new_number);
                    con.write(OutputKey, OutputValue);
                }
                

            }
        }
    }
