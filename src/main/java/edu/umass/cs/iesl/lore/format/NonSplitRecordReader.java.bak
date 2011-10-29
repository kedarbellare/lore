package edu.umass.cs.iesl.lore.format;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author kedarb
 * @since 10/23/11
 */
public class NonSplitRecordReader extends RecordReader<Text, Text> {
    private Text key = null;
    private Text value = null;
    private int count = 0;
    private FSDataInputStream fileIn;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        final Path file = split.getPath();
        key = new Text(file.toString());

        // open the file
        final FileSystem fs = file.getFileSystem(context.getConfiguration());
        fileIn = fs.open(file);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (count >= 1) return false;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int ch;
        while ((ch = fileIn.read()) != -1) baos.write(ch);
        byte[] data = baos.toByteArray();
        value = new Text(new String(data));
        count++;
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return count == 0 ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        fileIn.close();
    }
}
