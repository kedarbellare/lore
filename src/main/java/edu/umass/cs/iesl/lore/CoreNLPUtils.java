package edu.umass.cs.iesl.lore;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.trees.semgraph.SemanticGraphEdge;
import edu.stanford.nlp.util.CoreMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * @author kedarb
 * @since 10/23/11
 */
public class CoreNLPUtils {
    static final HashSet<String> ALLOWED_DEP_TAGS = new HashSet<String>(Arrays.asList(
            "NN", "NNS", "JJ", "VB", "VBD", "VBG", "VBN", "VBZ", "ADJ"));
    static final HashSet<String> NERS_TO_IGNORE = new HashSet<String>(Arrays.asList(
            "O", "TIME", "DURATION", "DATE", "NUMBER", "MONEY", "ORDINAL"));
    static final int MAX_PATH_LENGTH = 5;

    public static String getTag(CoreLabel token) {
        return token.get(CoreAnnotations.PartOfSpeechAnnotation.class);
    }

    public static String getNER(CoreLabel token) {
        return token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
    }

    public static String getWord(CoreLabel token) {
        return token.get(CoreAnnotations.TextAnnotation.class);
    }

    public static String getLemma(CoreLabel token) {
        return token.get(CoreAnnotations.LemmaAnnotation.class);
    }

    public static CoreMap getSentence(CorefChain.CorefMention mention, List<CoreMap> sentences) {
        return sentences.get(mention.sentNum - 1);
    }

    public static List<CoreLabel> getTokenList(CorefChain.CorefMention mention, List<CoreMap> sentences) {
        return getSentence(mention, sentences).get(CoreAnnotations.TokensAnnotation.class);
    }

    public static SemanticGraph getDependenciesForSentence(CoreMap sentence) {
        return sentence.get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
    }

    public static CoreLabel getHeadToken(CorefChain.CorefMention mention, List<CoreMap> sentences) {
        return getTokenList(mention, sentences).get(mention.headIndex - 1);
    }

    public static IndexedWord getIndexedWord(CorefChain.CorefMention mention, SemanticGraph dependencies) {
        return getIndexedWord(mention.headIndex, dependencies);
    }

    public static IndexedWord getIndexedWord(int headIndex, SemanticGraph dependencies) {
        for (IndexedWord idxword : dependencies.vertexList()) {
            if (headIndex == idxword.index())
                return idxword;
        }
        return dependencies.getFirstRoot();
    }

    public static String getMentionSpanForNER(CorefChain.CorefMention mention, List<CoreMap> sentences) {
        List<CoreLabel> tokenLst = getTokenList(mention, sentences);
        CoreLabel headToken = getHeadToken(mention, sentences);
        String headNer = getNER(headToken);
        if (headNer.equals("O")) return getWord(headToken);
        else {
            int begin = mention.headIndex;
            while (begin >= mention.startIndex && getNER(tokenLst.get(begin - 1)).equals(headNer)) {
                begin--;
            }
            begin++;
            int end = mention.headIndex;
            while (end < mention.endIndex && getNER(tokenLst.get(end - 1)).equals(headNer)) {
                end++;
            }
            StringBuilder builder = new StringBuilder();
            builder.append(getWord(tokenLst.get(begin - 1)));
            for (int i = begin + 1; i < end; i++) {
                builder.append(' ').append(getWord(tokenLst.get(i - 1)));
            }
            return builder.toString();
        }
    }

    public static List<String> getDependencies(CorefChain.CorefMention mention, List<CoreMap> sentences,
                                               HashSet<String> allowedDependencyTags, boolean useLabeledEdge) {
        CoreLabel headToken = getHeadToken(mention, sentences);
        SemanticGraph dependencies = getDependenciesForSentence(getSentence(mention, sentences));
        ArrayList<String> headDependencies = new ArrayList<String>();
        for (SemanticGraphEdge depEdge : dependencies.edgeList()) {
            IndexedWord depWord = depEdge.getDependent();
            IndexedWord govWord = depEdge.getGovernor();
            GrammaticalRelation relation = depEdge.getRelation();
            if (depWord.index() == headToken.index() &&
                    (allowedDependencyTags == null || allowedDependencyTags.contains(govWord.tag()))) {
                headDependencies.add("<-" + (useLabeledEdge ? relation : "") + "-" + govWord.lemma());
            }
            if (govWord.index() == headToken.index() &&
                    (allowedDependencyTags == null || allowedDependencyTags.contains(depWord.tag()))) {
                headDependencies.add("-" + (useLabeledEdge ? relation : "") + "->" + depWord.lemma());
            }
        }
        return headDependencies;
    }

    public static boolean isAllowedMentionNER(CorefChain.CorefMention mention, List<CoreMap> sentences) {
        String ner = getNER(getHeadToken(mention, sentences));
        return !NERS_TO_IGNORE.contains(ner);
    }

    public static List<String> getEdgeWalk(CorefChain.CorefMention src, CorefChain.CorefMention dest,
                                           List<CoreMap> sentences, boolean useLabeledEdge) {
        List<String> edgeWalked = new ArrayList<String>();
        // ignore different sentence mentions
        if (src.sentNum != dest.sentNum) return edgeWalked;
        // ignore relations between same entity
        if (src.corefClusterID == dest.corefClusterID) return edgeWalked;

        SemanticGraph dependencies = getDependenciesForSentence(sentences.get(src.sentNum - 1));
        IndexedWord isrc = getIndexedWord(src, dependencies);
        IndexedWord idest = getIndexedWord(dest, dependencies);
        List<SemanticGraphEdge> edges = dependencies.getShortestPathEdges(isrc, idest);
        // if no shortest path, return empty
        if (edges == null || edges.size() == 0 || edges.size() > MAX_PATH_LENGTH)
            return edgeWalked;

        // walk along edges
        int currIndex = isrc.index();
        for (SemanticGraphEdge edge : edges) {
            IndexedWord idep = edge.getDependent();
            IndexedWord igov = edge.getGovernor();
            // gov -> dep
            if (idep.index() == currIndex) {
                String edgeStr = "<-" + (useLabeledEdge ? edge.getRelation() : "") + "-";
                if (igov.index() != idest.index()) edgeWalked.add(edgeStr + igov.lemma());
                else edgeWalked.add(edgeStr);
                currIndex = igov.index();
            } else if (igov.index() == currIndex) {
                String edgeStr = "-" + (useLabeledEdge ? edge.getRelation() : "") + "->";
                if (idep.index() != idest.index()) edgeWalked.add(edgeStr + idep.lemma());
                else edgeWalked.add(edgeStr);
                currIndex = idep.index();
            }
        }
        return edgeWalked;
    }

    public static String getMentionUniqID(String filePath, CorefChain.CorefMention mention) {
        return filePath + "##" + mention.sentNum + "[" + mention.startIndex + "," + mention.endIndex + ")##head@" + mention.headIndex;
    }
}
