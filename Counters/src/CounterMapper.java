package src;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

enum LOCATION {
    TOTAL, BANGALORE, CHENNAI, HYDERABAD
}

public class CounterMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text vals, Context con) throws IOException, InterruptedException {

        // Static counter
        con.getCounter(LOCATION.TOTAL).increment(1);

        String line = vals.toString();
        String[] words = line.split(",");

        if (words[4].equalsIgnoreCase("bangalore")) {
            con.getCounter(LOCATION.BANGALORE).increment(1);
        } else if (words[4].equalsIgnoreCase("chennai")) {
            con.getCounter(LOCATION.CHENNAI).increment(1);
        } else {
            con.getCounter(LOCATION.HYDERABAD).increment(1);
        }

        // Dynamic counter
        int saleCount = Integer.parseInt(words[3]);
        int price = Integer.parseInt(words[2]);

        if (saleCount < 10) {
            con.getCounter("SALE", "LOW_SALES").increment(1);
        }

        if (saleCount * price > 500) {
            con.getCounter("SALE", "HIGH_REVENUE").increment(1);
        }

        Text loc = new Text();
        Text data = new Text();

        loc.set(words[4]);
        data.set(words[2] + " , " + words[3]);

        con.write(loc, data);

    }
}