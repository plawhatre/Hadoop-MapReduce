package src;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class JoinMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private HashMap<String, String> product = new HashMap<>();
    private HashMap<String, String> store = new HashMap<>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        URI[] cacheFile = context.getCacheFiles();

        if (cacheFile != null && cacheFile.length > 0) {
            //
            try {
                //
                String line = "";
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath1 = new Path(cacheFile[0].toString());
                Path getFilePath2 = new Path(cacheFile[1].toString());

                // Product
                BufferedReader myReader1 = new BufferedReader(
                        new InputStreamReader(fs.open(getFilePath1)));

                while ((line = myReader1.readLine()) != null) {
                    String[] words = line.split(",");
                    product.put(words[0].trim(),
                            words[3].trim());
                }

                myReader1.close();

                // Store
                BufferedReader myReader2 = new BufferedReader(
                        new InputStreamReader(fs.open(getFilePath2)));

                while ((line = myReader2.readLine()) != null) {
                    String[] words = line.split(",");
                    store.put(words[0].trim(),
                            words[1].trim());
                }

                myReader2.close();

            } catch (FileNotFoundException e) {
                System.out.println("An error occured");
            }
        }
    }

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        //
        String line = values.toString();
        String[] words = line.split(",");

        int productSale = Integer.parseInt(words[3].trim());
        double productPrice = Double.parseDouble(
                product.get(words[1]).toString().trim());
        double revenue = productPrice * productSale;

        String storeID = words[0].toString().trim();
        String loc = store.get(storeID);

        context.write(new Text(storeID + " " + loc),
                new DoubleWritable(revenue));
    }
}