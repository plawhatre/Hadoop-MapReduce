package src;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class joinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "multi-input");
        job.setJarByClass(joinDriver.class);
        job.setMapperClass(joinMapper1.class);
        job.setMapperClass(joinMapper2.class);
        job.setReducerClass(joinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        String[] files = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        Path input1 = new Path(files[0]);
        Path input2 = new Path(files[1]);
        Path output = new Path(files[2]);

        MultipleInputs.addInputPath(job, input1, TextInputFormat.class, joinMapper1.class);
        MultipleInputs.addInputPath(job, input2, TextInputFormat.class, joinMapper2.class);
        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}