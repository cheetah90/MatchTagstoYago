package utils;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;

import java.io.StringReader;
import java.util.List;

public class NLPUtils {
    private static String buildSentenceFromHasWord(List<HasWord> sentence) {
        // this is directly from https://github.com/stanfordnlp/CoreNLP/blob/master/src/edu/stanford/nlp/process/DocumentPreprocessor.java
        StringBuilder stringBuilder = new StringBuilder();
        boolean printSpace = false;

        for (HasWord word : sentence) {
            if (printSpace) stringBuilder.append(" ");
            printSpace = true;
            stringBuilder.append(word.word());
        }

        return stringBuilder.toString();
    }

    private static String getFirstSentence(String strDescriptions) {
        strDescriptions = strDescriptions.split("\n")[0];
        StringReader srDescription = new StringReader(strDescriptions);
        DocumentPreprocessor dp = new DocumentPreprocessor(srDescription);
        String firstSentence = buildSentenceFromHasWord(dp.iterator().next());


        if (!firstSentence.isEmpty()) {
            return firstSentence;
        }

        return strDescriptions;
    }

    public synchronized static String getFirstPhrase(String strDescriptions) {
        if (strDescriptions!=null && !strDescriptions.equals("")){
            String firstPhrase = getFirstSentence(strDescriptions);
            firstPhrase = firstPhrase.split("\\.")[0].trim();
            firstPhrase = firstPhrase.split(",")[0].trim();
            firstPhrase = firstPhrase.split(";")[0].trim();
            if (firstPhrase.contains(":")) {
                if (firstPhrase.endsWith(":")) {
                    // if it ends with ":", just strip it off
                    firstPhrase = firstPhrase.substring(0, firstPhrase.length()-1);
                } else {
                    // If it has split
                    firstPhrase = firstPhrase.split(":")[1].trim();
                }
            }

            return firstPhrase;
        }

        return "";
    }
}
