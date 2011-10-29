package edu.umass.cs.iesl.lore;

import java.io.IOException;
import java.util.*;

import com.nytlabs.corpus.NYTCorpusDocument;
import com.nytlabs.corpus.NYTCorpusDocumentParser;
import edu.umass.cs.iesl.lore.format.NonSplitFileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * @author kedarb
 * @since 10/23/11
 */
public class NYTDocumentToText {
    public static class Map extends Mapper<Text, Text, NullWritable, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setValidating(false);
            try {
                String xmlStr = value.toString();
                xmlStr = xmlStr.replace("<!DOCTYPE nitf "
                        + "SYSTEM \"http://www.nitf.org/IPTC/NITF/3.3/specification/dtd/nitf-3-3.dtd\">", "");
                Document xmlDoc = factory.newDocumentBuilder().parse(xmlStr);
                NYTCorpusDocumentParser parser = new NYTCorpusDocumentParser();
                NYTCorpusDocument nytDoc = parser.parseNYTCorpusDocumentFromDOMDocument(null, xmlDoc);
                context.write(NullWritable.get(), new Text(nytDoc.getBody()));
            } catch (ParserConfigurationException pce) {
                throw new IOException(pce.getMessage(), pce);
            } catch (SAXException saxe) {
                throw new IOException(saxe.getMessage(), saxe);
            }
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: nyt-to-text <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "nyt-to-text");
        job.setJarByClass(NYTDocumentToText.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setInputFormatClass(NonSplitFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(0);

        System.out.println("input=" + args[0] + " output=" + args[1]);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
