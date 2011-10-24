package edu.umass.cs.iesl.lore;


import com.nytlabs.corpus.NYTCorpusDocument;
import com.nytlabs.corpus.NYTCorpusDocumentParser;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;

/**
 * @author kedarb
 * @since 10/23/11
 */
public class NYTXmlToText {
    public static void xml2text(File file) throws IOException {
        if (file.isDirectory()) {
            for (File ifile : file.listFiles()) xml2text(ifile);
        } else if (file.isFile() && file.getName().endsWith(".xml")) {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            docBuilderFactory.setValidating(false);
            try {
                Document doc = docBuilderFactory.newDocumentBuilder().parse(file);
                NYTCorpusDocumentParser parser = new NYTCorpusDocumentParser();
                NYTCorpusDocument nytDoc = parser.parseNYTCorpusDocumentFromDOMDocument(file, doc);
                // create output file
                String outputFilename = file.getAbsolutePath().replace("/data/", "/processed/").replaceAll("\\.xml$", ".txt");
                File outputFile = new File(outputFilename);
                // create parent directory if non-existent
                outputFile.getParentFile().mkdirs();
                // write body text to file
                PrintWriter writer = new PrintWriter(outputFile);
                writer.println(nytDoc.getBody());
                writer.flush();
                writer.close();
                System.out.println("Finished: " + file.getAbsolutePath() + " -> " + outputFilename);
            } catch (ParserConfigurationException pce) {
                pce.printStackTrace();
            } catch (SAXException saxe) {
                saxe.printStackTrace();
            }
        }
    }

    public static void main(String args[]) throws IOException {
        for (String filename : args) {
            xml2text(new File(filename));
        }
    }
}
