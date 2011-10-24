package edu.umass.cs.iesl.lore;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CorefCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import edu.umass.cs.iesl.lore.format.NonSplitFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

/**
 * @author kedarb
 * @since 10/23/11
 */
public class RelationMentionPatternExtractor {
    public static class Map extends Mapper<Text, Text, Text, Text> {
        // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
        static StanfordCoreNLP pipeline;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            initPipeline();
        }

        private static void initPipeline() {
            if (pipeline == null) {
                Properties props = new Properties();
                props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
                pipeline = new StanfordCoreNLP(props);
            }
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String filePath = key.toString();
            String docText = value.toString();
            initPipeline();
            Annotation document = new Annotation(docText);
            pipeline.annotate(document);

            // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

            // This is the coreference link graph
            // Each chain stores a set of mentions that link to each other,
            // along with a method for getting the most representative mention
            // Both sentence and token offsets start at 1!
            java.util.Map<Integer, CorefChain> graph = document.get(CorefCoreAnnotations.CorefChainAnnotation.class);
            for (int srcChainNum : graph.keySet()) {
                CorefChain srcChain = graph.get(srcChainNum);
                CorefChain.CorefMention srcEntity = srcChain.getRepresentativeMention();
                if (!CoreNLPUtils.isAllowedMentionNER(srcEntity, sentences))
                    continue;

                String srcEntityPhrase = CoreNLPUtils.getMentionSpanForNER(srcEntity, sentences);
                String srcNER = CoreNLPUtils.getNER(CoreNLPUtils.getHeadToken(srcEntity, sentences));
                String srcEntityId = srcEntityPhrase + "##" + CoreNLPUtils.getMentionUniqID(filePath, srcEntity);
                for (CorefChain.CorefMention srcMention : srcChain.getCorefMentions()) {
                    String srcMentionId = CoreNLPUtils.getMentionUniqID(filePath, srcMention);
                    String srcPhrase = CoreNLPUtils.getMentionSpanForNER(srcMention, sentences);

                    for (int destChainNum : graph.keySet()) {
                        CorefChain destChain = graph.get(destChainNum);
                        CorefChain.CorefMention destEntity = destChain.getRepresentativeMention();
                        if (!CoreNLPUtils.isAllowedMentionNER(destEntity, sentences))
                            continue;

                        String destEntityPhrase = CoreNLPUtils.getMentionSpanForNER(destEntity, sentences);
                        String destNER = CoreNLPUtils.getNER(CoreNLPUtils.getHeadToken(destEntity, sentences));
                        String destEntityId = destEntityPhrase + "##" + CoreNLPUtils.getMentionUniqID(filePath, destEntity);
                        for (CorefChain.CorefMention destMention : destChain.getCorefMentions()) {
                            String destMentionId = CoreNLPUtils.getMentionUniqID(filePath, destMention);
                            String destPhrase = CoreNLPUtils.getMentionSpanForNER(destMention, sentences);
                            List<String> edgeWalk = CoreNLPUtils.getEdgeWalk(srcMention, destMention, sentences, true);
                            if (edgeWalk.size() > 0) {
                                StringBuilder edgeWalkBuff = new StringBuilder();
                                for (String edge : edgeWalk) edgeWalkBuff.append(edge);
                                String tsvalue = srcEntityPhrase + "\t" + srcEntityId + "\t" + srcNER + "\t" +
                                        srcPhrase + "\t" + srcMentionId + "\t" +
                                        edgeWalkBuff.toString() + "\t" +
                                        destEntityPhrase + "\t" + destEntityId + "\t" + destNER + "\t" +
                                        destPhrase + "\t" + destMentionId;
                                context.write(key, new Text(tsvalue));
                            }
                        }
                    }
                }
            }
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: text-to-mentions <in> <out>");
            System.exit(2);
        }

        conf.set("mapred.child.java.opts", "-Xmx5G");

        Job job = new Job(conf, "text-to-mentions");
        job.setJarByClass(RelationMentionPatternExtractor.class);

        job.setOutputKeyClass(Text.class);
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
