package edu.umass.cs.iesl.lore.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;

/**
 * @author kedarb
 * @since 10/23/11
 */
public class NonSplitFileInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new NonSplitRecordReader();
    }

//    @Override
//    protected long getFormatMinSplitSize() {
//        return Long.MAX_VALUE;
//    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
