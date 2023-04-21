package src;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class XmlRecordReader extends RecordReader<LongWritable, Text> {

    private final String startTag = "<MOVIES>";
    private final String endTag = "<MOVIES>";

    private LineReader linereader;

    private long currentPosition = 0;
    private long startOfFile;
    private long endOfFile;

    private LongWritable key = new LongWritable();
    private Text value = new Text();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) inputSplit;
        Configuration conf = context.getConfiguration();

        startOfFile = fileSplit.getStart();
        endOfFile = startOfFile + fileSplit.getLength();

        Path filePath = fileSplit.getPath();
        FileSystem fileSystem = filePath.getFileSystem(conf);

        FSDataInputStream fsInputStream = fileSystem.open(filePath);
        fsInputStream.seek(startOfFile);
        linereader = new LineReader(fsInputStream, conf);

        this.currentPosition = startOfFile;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //
        key.set(currentPosition);
        value.clear();

        Text line = new Text();
        boolean startFound = false;

        while (currentPosition < endOfFile) {
            long lineLength = linereader.readLine(line);
            currentPosition += lineLength;

            if (!startFound && line.toString().equalsIgnoreCase(this.startTag)) {

                startFound = true;

            } else if (startFound && line.toString().equalsIgnoreCase(this.endTag)) {

                String withoutComma = value.toString().substring(0, value.toString().length() - 1);
                value.set(withoutComma);
                return true;

            } else if (startFound) {

                String s1 = line.toString();
                String content = s1.replaceAll("<[^>]+>", "");
                value.append(content.getBytes("utf-8"), 0, content.length());
                value.append(",".getBytes("utf-8"), 0, ",".length());

            } 
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    // Following is not important: just for tracking
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (currentPosition - startOfFile) / (float) (endOfFile - startOfFile);
    }

    @Override
    public void close() throws IOException {
        if (linereader != null) {
            linereader.close();
        }
    }
}
