package src;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WCDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Job job = Job.getInstance(conf, "multi-input");
        job.setJarByClass(WCDriver.class);
        job.setMapperClass(WCMapper1.class);
        job.setMapperClass(WCMapper2.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        String[] files = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        Path input1 = new Path(files[0]);
        Path input2 = new Path(files[1]);
        Path output = new Path(files[2]);

        MultipleInputs.addInputPath(job, input1, TextInputFormat.class, WCMapper1.class);
        MultipleInputs.addInputPath(job, input2, KeyValueTextInputFormat.class, WCMapper2.class);
        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}