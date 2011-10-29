package edu.umass.cs.iesl.lore;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CorefCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author kedarb
 * @since 10/23/11
 */
public class CoreNLPAnnotator {
    // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
    StanfordCoreNLP pipeline;

    public CoreNLPAnnotator() {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        pipeline = new StanfordCoreNLP(props);
    }

    public StanfordCoreNLP getPipeline() {
        return pipeline;
    }

    /**
     * Schema: [filePath] [reprEntityPhrase] [reprEntityId] [reprHeadNER] [reprDependencies]
     * @param filePath
     * @param docText
     * @return
     */
    public List<String> getEntityMentionPatterns(String filePath, String docText) {
        List<String> entPatterns = new ArrayList<String>();
        Annotation document = new Annotation(docText);
        pipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        // This is the coreference link graph
        // Each chain stores a set of mentions that link to each other,
        // along with a method for getting the most representative mention
        // Both sentence and token offsets start at 1!
        Map<Integer, CorefChain> graph = document.get(CorefCoreAnnotations.CorefChainAnnotation.class);
        for (int chainNum : graph.keySet()) {
            CorefChain chain = graph.get(chainNum);
            CorefChain.CorefMention reprMention = chain.getRepresentativeMention();
            if (CoreNLPUtils.isAllowedMentionNER(reprMention, sentences)) {
                String reprPhrase = CoreNLPUtils.getMentionSpanForNER(reprMention, sentences);
                String reprHeadNER = CoreNLPUtils.getNER(CoreNLPUtils.getHeadToken(reprMention, sentences));
                String reprEntityId = reprPhrase.replace(' ', '_') + "##" + CoreNLPUtils.getMentionUniqID(filePath, reprMention);
                ArrayList<String> reprMentionDeps = new ArrayList<String>();
                for (CorefChain.CorefMention mention : chain.getCorefMentions()) {
                    List<String> mentionDeps = CoreNLPUtils.getDependencies(mention, sentences, CoreNLPUtils.ALLOWED_DEP_TAGS, true);
                    reprMentionDeps.addAll(mentionDeps);
                }
                if (reprMentionDeps.size() > 0) {
                    String reprDepStr = StringUtils.join(reprMentionDeps, "|||");
                    String tsvalue = filePath + "\t" + reprPhrase + "\t" + reprEntityId + "\t" + reprHeadNER + "\t" + reprDepStr;
                    entPatterns.add(tsvalue);
                }
            }
        }

        return entPatterns;
    }

    public List<String> getRelationMentionPatterns(String filePath, String docText) {
        List<String> relPatterns = new ArrayList<String>();
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
            String srcEntityId = srcEntityPhrase.replace(' ', '_') + "##" + CoreNLPUtils.getMentionUniqID(filePath, srcEntity);
            for (int destChainNum : graph.keySet()) {
                CorefChain destChain = graph.get(destChainNum);
                CorefChain.CorefMention destEntity = destChain.getRepresentativeMention();
                if (!CoreNLPUtils.isAllowedMentionNER(destEntity, sentences))
                    continue;

                String destEntityPhrase = CoreNLPUtils.getMentionSpanForNER(destEntity, sentences);
                String destNER = CoreNLPUtils.getNER(CoreNLPUtils.getHeadToken(destEntity, sentences));
                String destEntityId = destEntityPhrase.replace(' ', '_') + "##" + CoreNLPUtils.getMentionUniqID(filePath, destEntity);
                ArrayList<String> srcDestEdgeWalks = new ArrayList<String>();

                for (CorefChain.CorefMention srcMention : srcChain.getCorefMentions()) {
                    // String srcMentionId = CoreNLPUtils.getMentionUniqID(filePath, srcMention);
                    // String srcPhrase = CoreNLPUtils.getMentionSpanForNER(srcMention, sentences);

                    for (CorefChain.CorefMention destMention : destChain.getCorefMentions()) {
                        // String destMentionId = CoreNLPUtils.getMentionUniqID(filePath, destMention);
                        // String destPhrase = CoreNLPUtils.getMentionSpanForNER(destMention, sentences);
                        List<String> edgeWalk = CoreNLPUtils.getEdgeWalk(srcMention, destMention, sentences, true);
                        if (edgeWalk.size() > 0) {
                            srcDestEdgeWalks.add(StringUtils.join(edgeWalk, ""));
                        }
                    }
                }

                if (srcDestEdgeWalks.size() > 0) {
                    String srcDestEdgeWalkStr = StringUtils.join(srcDestEdgeWalks, "|||");
                    String tsvalue = filePath + "\t" + srcEntityPhrase + "\t" + srcEntityId + "\t" + srcNER + "\t" +
                            srcDestEdgeWalkStr + "\t" + destEntityPhrase + "\t" + destEntityId + "\t" + destNER;
                    relPatterns.add(tsvalue);
                }
            }
        }

        return relPatterns;
    }

    public static void main(String args[]) {
        CoreNLPAnnotator annotator = new CoreNLPAnnotator();
        String text = "President Jacques Chirac has demanded that the United States sign both the Kyoto climate protocol " +
                "and a future agreement that will take effect when the Kyoto accord runs out in 2012.\n" +
                "He said that he welcomed last week's State of the Union address in which President Bush described climate " +
                "change as a ''serious challenge'' and acknowledged that a growing number of American politicians now " +
                "favor emissions cuts.\n" +
                "But he warned that if the United States did not sign the agreements, a carbon tax across Europe on imports " +
                "from nations that have not signed the Kyoto treaty could be imposed to try to force compliance. The European " +
                "Union is the largest export market for American goods.\n" +
                "''A carbon tax is inevitable,'' Mr. Chirac said. ''If it is European, and I believe it will be European, " +
                "then it will all the same have a certain influence because it means that all the countries that do not " +
                "accept the minimum obligations will be obliged to pay.''\n" +
                "Trade lawyers have been divided over the legality of a carbon tax, with some saying it would run counter " +
                "to international trade rules. But Mr. Chirac said other European countries would back it. " +
                "''I believe we will have all of the European Union,'' he said.\n" +
                "Mr. Chirac spoke as scientists from around the world gathered in Paris to discuss an authoritative " +
                "international report on climate change, portions of which will be released on Friday.\n" +
                "Mr. Chirac's critics say that despite his comments in support of environmental measures, his record as " +
                "president is far from green. He angered environmentalists across the globe when he conducted nuclear tests " +
                "in a Pacific atoll within months of coming into office in 1995. He has been a loyal ally of French farmers " +
                "and their pollution-causing practices, blocking some proposed Europe-wide reforms.\n" +
                "Most recently, France's national plan for allocating carbon emission credits to businesses had to be " +
                "revised after the European Union rejected it as too generous.";

        Annotation document = new Annotation(text);
        annotator.getPipeline().annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        // This is the coreference link graph
        // Each chain stores a set of mentions that link to each other,
        // along with a method for getting the most representative mention
        // Both sentence and token offsets start at 1!
        Map<Integer, CorefChain> graph = document.get(CorefCoreAnnotations.CorefChainAnnotation.class);
        for (int srcChainNum : graph.keySet()) {
            CorefChain srcChain = graph.get(srcChainNum);
            CorefChain.CorefMention srcEntity = srcChain.getRepresentativeMention();
            if (!CoreNLPUtils.isAllowedMentionNER(srcEntity, sentences))
                continue;

            String srcPhrase = CoreNLPUtils.getMentionSpanForNER(srcEntity, sentences);
            for (CorefChain.CorefMention srcMention : srcChain.getCorefMentions()) {
                for (int destChainNum : graph.keySet()) {
                    CorefChain destChain = graph.get(destChainNum);
                    CorefChain.CorefMention destEntity = destChain.getRepresentativeMention();
                    if (!CoreNLPUtils.isAllowedMentionNER(destEntity, sentences))
                        continue;

                    String destPhrase = CoreNLPUtils.getMentionSpanForNER(destEntity, sentences);
                    for (CorefChain.CorefMention destMention : destChain.getCorefMentions()) {
                        List<String> edgeWalk = CoreNLPUtils.getEdgeWalk(srcMention, destMention, sentences, false);
                        if (edgeWalk.size() > 0) {
                            System.out.println(srcPhrase + edgeWalk + destPhrase);
                        }
                    }
                }
            }
        }
    }
}
