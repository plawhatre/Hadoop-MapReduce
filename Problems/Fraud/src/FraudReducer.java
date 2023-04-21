package src;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class FraudReducer extends Reducer<Text, FraudWritable, Text, IntWritable>{

    ArrayList<String> customers = new ArrayList<>();

    public void reduce(Text key, Iterable<FraudWritable> values, Context con) throws IOException, InterruptedException{
        
        int fraudPts = 0;
        int returnedCount = 0;
        int ordersCount = 0;

        FraudWritable temp = new FraudWritable();

        for (FraudWritable value : values) {
            
            ordersCount += 1;

            if (value.getReturned()){
                try{
                    returnedCount += 1;
                    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
                    Date receiveDate = sdf.parse(value.getReceivedDate());
                    Date returnDate = sdf.parse(value.getReturnDate());

                    long diffInMilliSec = Math.abs(returnDate.getTime() - receiveDate.getTime());
                    long diffDays = TimeUnit.DAYS.convert(diffInMilliSec, TimeUnit.DAYS);
                    
                    if (diffDays > 10) {
                        fraudPts += 1;
                    }

                } catch (Exception e){

                    e.printStackTrace();
                }

            }
            temp = value;
        }
        // 10 fraud points if return rate > 50%
        float rate = returnedCount / (ordersCount*Float.parseFloat("1")) ;

        if (rate >= 0.5){
            fraudPts += 10;
        }
        // con.write(key, new Text(","+temp.getName()+','+fraudPts));
        customers.add(key.toString()+","+temp.getName()+','+fraudPts);
    }

    @Override
    public void cleanup(Context con) throws IOException, InterruptedException {
        Collections.sort(customers, new Comparator<String>(){
            public int compare(String s1, String s2){
                int fp1 = Integer.parseInt(s1.split(",")[2]);
                int fp2 = Integer.parseInt(s2.split(",")[2]);

                return fp2 - fp1;
            }
        });
       
        for (String customer: customers){
            String[] words = customer.split(",");
            con.write(new Text(words[0]+","+words[1]), 
                      new IntWritable(Integer.parseInt(words[2])));
        }
    }
}