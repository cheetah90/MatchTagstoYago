import basics.FactComponent;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.translate.*;
import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedSummaryStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.NLPUtils;

import java.io.*;
import java.sql.Connection;
import java.util.*;

public class ProcessSingleImageRunnable implements Runnable {
    /**
     * Getters and Setters
     */

    public static void setPreferredMeanings(Map<String, String> preferredMeanings) {
        ProcessSingleImageRunnable.preferredMeanings = preferredMeanings;
    }

    public static void setNonConceptualCategories(Set<String> nonConceptualCategories) {
        ProcessSingleImageRunnable.nonConceptualCategories = nonConceptualCategories;
    }

    public static void setYagoConnection(Connection yagoConnection) {
        ProcessSingleImageRunnable.yagoConnection = yagoConnection;
    }

    public static void setYagoLowercase2Original(HashMap<String, HashSet<String>> yagoLowercase2Original) {
        ProcessSingleImageRunnable.yagoLowercase2Original = yagoLowercase2Original;
    }

    public static void setYagoOriginal2Type(HashMap<String, HashSet<String>> yagoOriginal2Type) {
        ProcessSingleImageRunnable.yagoOriginal2Type = yagoOriginal2Type;
    }

    public static int getCompletedCounter() {
        return completedCounter;
    }

    private synchronized static void incrementCompletedCounter() {
        completedCounter++;
    }

    private synchronized static void incrementStartedCounter() {
        startedCounter++;
    }

    private synchronized static void incrementFlickrCounter() {
        FlickrCounter++;
    }

    private synchronized static void incrementPanoramioCounter() {
        PanoramioCounter++;
    }

    public static int getFlickrCounter(){
        return FlickrCounter;
    }

    public static int getPanoramioCounter(){
        return PanoramioCounter;
    }

    /** Holds the preferred meanings */
    private static Map<String, String> preferredMeanings;

    // Holds the nonconceptual categorie
    private static Set<String> nonConceptualCategories;

    private static Connection yagoConnection;

    private static HashMap<String, HashSet<String>> yagoLowercase2Original;

    private static HashMap<String, HashSet<String>> yagoOriginal2Type;

    private static final Logger logger = LogManager.getLogger(ProcessSingleImageRunnable.class);

    private static int completedCounter = 0;

    private static int startedCounter = 0;

    private static final int MAXTOKENSINAPHRASE = 11;

    private static final int MINCHARSINTITLE= 5;

    private static int FlickrCounter = 0;

    private static int PanoramioCounter = 0;

    private static LanguageDetector languageDetector;

    private static TextObjectFactory textObjectFactory;

    private static final Object translationLock = new Object();

    // DEBUG: for debug purpose only
    static final SummaryStatistics time_mediaWikipeida = new SynchronizedSummaryStatistics();

    static final SummaryStatistics time_preprocessCommonsMetadata = new SynchronizedSummaryStatistics();

    static final SummaryStatistics time_processOneCategory = new SynchronizedSummaryStatistics();

    static final SummaryStatistics time_processOneDescription = new SynchronizedSummaryStatistics();

    static final SummaryStatistics time_oneImage = new SynchronizedSummaryStatistics();

    /**
     * Finish static methods and variables
     */

    class TranslationResults {
        String getOriginalText() {
            return originalText;
        }

        String getTranslatedText() {
            return translatedText;
        }

        public String getLang() {
            return lang;
        }

        private String originalText;
        private String translatedText;
        private String lang;

        TranslationResults(String originalText, String translatedText, String lang){
            this.originalText = originalText;
            this.translatedText = translatedText;
            this.lang = lang;
        }
    }

    private MediaWikiCommonsAPI mediaWikiCommonsAPI;

    private Translate googleTranslate;

    private String original_title;

    private boolean isFlickr = false;

    private boolean isPanoramio = false;

    private MatchCategory matchCategory;


    private static boolean isNotPhoto(String fileName) {
        return (fileName.contains(".webm") || fileName.contains(".ogg"));
    }

    private static final String[] BLACKLIST_STARTWITH_CATEGORIES = { "commons", "cc-", "pd_", "categories_", "items_with", "attribution_", "gfdl", "pd-", "files_using_", "files_",
            "photos_of_", "photos,_created_", "media_missing_", "projet_québec", "work_", "scans_", "scan_"};

    private static final String[] BLACKLIST_CONTAINS_CATEGORIES ={"copyright", "license", "media_type", "file_format", "media_needing", "flickr", "self-published_work", "images_by", "by_user",
            "images_from", "panorami", "images_in", "photos_by", "images_by", "upload", "geograph_images", "personality_rights_warning", "media_lacking",
            "media_supported_by", "media_by", "media_from", "media_with", "files_from", "pages_with_map", "media_contributed_by", "user:", "files_created_by",
            "photograph", "wikidata", "taken_with", "robert_d._ward", "nike_specific_patterns", "files_with_no", "template_unknown", "_temp_", "department_of_", "supported_by_",
            "images_with_", "files_by", "lgpl", "protected_", "wikipedia", "photos_from", "media_donated_by", "nature_neighbors", "_locations_", "photos,_created_by_", "project_",
            "djvu_files", "images_of_", "gerard_dukker", "wikimania", "translation_possible", "attribute_", "image_description", "wikiafrica_", "_view_", "_views_",
            "elef_milim", "_work_", "_scan_", "extracted_images"
    };

    private static final String[] BLACKLIST_EQUAL_CATEGORIES={"fal", "attribution", "retouched_pictures"
    };


    ProcessSingleImageRunnable(String original_title) {
        this.original_title = original_title;

        try {
            // Set up the Google Translate API connection
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("./src/main/resources/wikicommons-1c391c623d29.json"));
            this.googleTranslate = TranslateOptions.newBuilder().setCredentials(credentials).build().getService();

            // Set up the MediaWikiCommons API
            this.mediaWikiCommonsAPI = new MediaWikiCommonsAPI();

            // Use local language detector
            if (MatchYago.getPROPERTIES().getProperty("useLocalLangDetector").equals("true")) {
                //load the static language detector if not already:
                if (languageDetector == null && textObjectFactory == null) {
                    List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();

                    //build language detector:
                    languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                            .withProfiles(languageProfiles)
                            .build();

                    //create a text object factory
                    textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
                }

            }
        } catch (Exception exception) {
            logger.error(exception.getStackTrace());
        }

        if (MatchYago.getPROPERTIES().getProperty("LoadYago2Memory").equals("true")) {
            this.matchCategory = new MatchCategory(preferredMeanings, nonConceptualCategories, yagoLowercase2Original, yagoOriginal2Type);
        } else {
            this.matchCategory = new MatchCategory(preferredMeanings, nonConceptualCategories, yagoConnection);
        }

    }

    private String preprocessTitle(String title) {
        String proper_title = FilenameUtils.removeExtension(title);

        int pos = 0;

        // trim the numbers from left
        while (pos < proper_title.length()){
            if (Character.isLetter(proper_title.charAt(pos))) {
                proper_title = proper_title.substring(pos);
                break;
            }
            pos ++;
        }

        // trim the numbers from right
        pos = proper_title.length()-1;
        while (pos >= 0) {
            if (Character.isLetter(proper_title.charAt(pos))) {
                proper_title = proper_title.substring(0, pos+1);
                break;
            }
            pos --;
        }

        return proper_title;
    }

    private boolean isWhiltelist(String current_category){
        return (current_category.contains("Aerial_photograph") ||
                current_category.contains("Copyright_symbols") ||
                current_category.contains("Microscopic_image")
        );
    }

    private boolean isValidNounGroup(String current_category) {
        // update Flickr and Panoramio counter accordingly
        if (current_category.toLowerCase().contains("flickr")) {
            isFlickr = true;
        }

        if (current_category.toLowerCase().contains("panoramio")) {
            isPanoramio = true;
        }

        //This is a hack to deal with whitelist
        if (isWhiltelist(current_category)){
            return true;
        }

        if (isNotBlacklist(current_category)) {
            return true;
        }

        return false;

    }

    private boolean isNotBlacklist(String cc){
        cc = cc.toLowerCase();

        for (String startwith_bl: BLACKLIST_STARTWITH_CATEGORIES){
            if (cc.startsWith(startwith_bl)) {
                return false;
            }
        }

        for (String contain_bl: BLACKLIST_CONTAINS_CATEGORIES){
            if (cc.contains(contain_bl)) {
                return false;
            }
        }

        for (String equal_bl: BLACKLIST_EQUAL_CATEGORIES){
            if (cc.equals(equal_bl)){
                return false;
            }
        }

        return true;
    }


    /**
     * Preprocess the metadata. It does the following:
     * In category:
     *      if contains "/" or " - ", split this category into two categories
     *      filter the topical category
     * In description:
     *
     * @param metadata
     */
    private void preprocessCommonsMetadata(MediaWikiCommonsAPI.CommonsMetadata metadata) {
        // preprocess categories
        ArrayList<String> ultimate_category = new ArrayList<>();
        ArrayList<String> additional_category = new ArrayList<>();

        for (String current_category: metadata.getCategories()) {
            //Split the category with slash e.g.  Football kit body/Nike specific patterns. Only use the first half
            if (current_category.contains("/")) {
                additional_category.add(current_category.split("/")[0]);
                continue;
            }

            //Split the category with (space)dash(space)
            if (current_category.contains("_-_")){
                additional_category.addAll(Arrays.asList(current_category.split("_-_")));
                continue;
            }

            // process this normal category
            if (isValidNounGroup(current_category)){
                ultimate_category.add(current_category);
            }
        }

        // process the additional
        for (String current_category: additional_category) {
            if (isValidNounGroup(current_category)){
                ultimate_category.add(current_category);
            }
        }
        metadata.setCategories(ultimate_category);


        // preprocess descriptions
        // remove substring inside parenthesis since it cause problem with CoreNLP parsing results.
        // defer calling isValidNounGroup to later since parsing the first phrase of description needs Google Translation.
        metadata.setDescription(metadata.getDescription().replaceAll("\\s*\\([^\\)]*\\)\\s*", " ").trim());


        // preprocess title
        metadata.setTitle(preprocessTitle(metadata.getTitle()));

    }

    private ProcessSingleImageRunnable.TranslationResults translateToEnglish(String original_text){
        String strip_original = FactComponent.stripCat(original_text).trim();
        strip_original = strip_original.startsWith("\n")?strip_original.substring("\n".length()):strip_original;
        // Just use the first paragraph in case the text is a combination of English and foreign language
        // e.g.  description in https://commons.wikimedia.org/wiki/File:Matereialseilbahn_Dotternhausen_22022014.JPG
        if (strip_original.contains("\n")) {
            strip_original = strip_original.split("\n")[0];
        }
        String englishText = original_text;

        String lang;
        // Use local language detector
        if (MatchYago.getPROPERTIES().getProperty("useLocalLangDetector").equals("true")) {
            // Synchronized this block since it uses static methods and variables
            synchronized (translationLock) {
                TextObject textObject = textObjectFactory.forText(strip_original);
                Optional<LdLocale> langOptional = languageDetector.detect(textObject);
                lang = langOptional.isPresent()?langOptional.get().getLanguage():"en";
            }

            // translate oc to fr
            lang = lang.equals("oc")?"fr":lang;
        } else {
            Detection detection = googleTranslate.detect(strip_original);
            lang = detection.getLanguage();
        }


        try {
            if (! lang.equals("en")) {
                Translation translation = googleTranslate.translate(
                        strip_original,
                        Translate.TranslateOption.sourceLanguage(lang),
                        Translate.TranslateOption.targetLanguage("en"));

                englishText = translation.getTranslatedText();
            }
        } catch (TranslateException exception) {
            logger.error(exception.getStackTrace());
            englishText = "";
        }

        return (new ProcessSingleImageRunnable.TranslationResults(original_text, englishText, lang));
    }

    private String matchNounPhraseTranslation2Yago (TranslationResults translationResults) {
        String yago_match = null;

        // To deal with foreign proper noun, direct match lang_code + original_text
        if (!translationResults.getLang().equals("en")) {
            yago_match = matchCategory.directMatch(translationResults.getLang().toLowerCase() + "/" + translationResults.getOriginalText().toLowerCase());
            if (matchCategory.isValidYagoItem(yago_match, translationResults.getLang().toLowerCase() + "/" + translationResults.getOriginalText().toLowerCase())) {
                return yago_match;
            }

            // match the foreign text without lang code
            yago_match = matchCategory.directMatch(translationResults.getOriginalText().toLowerCase());
            if (matchCategory.isValidYagoItem(yago_match, translationResults.getOriginalText().toLowerCase())) {
                return yago_match;
            }
        }

        // Parse the English NounPhrase
        yago_match = matchCategory.matchNounGroup2Yago(translationResults.getTranslatedText());
        if (matchCategory.isValidYagoItem(yago_match, translationResults.getTranslatedText())){
            return yago_match;
        }

        return null;
    }

    private void appendLinetoFile(String strLine, String outputFileName){
        try {
            Writer output = new BufferedWriter(new FileWriter(outputFileName, true));
            output.append(strLine);
            output.append("\n");
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private TranslationResults getFirstPhraseTranslationResults(TranslationResults translation) {
        String firstPhraseEng = NLPUtils.getFirstPhrase(translation.getTranslatedText()).trim().replaceAll(" ","_");
        String firstPhraseOriginal = NLPUtils.getFirstPhrase(translation.getOriginalText()).trim().replaceAll(" ", "_");

        return new TranslationResults(firstPhraseOriginal, firstPhraseEng, translation.getLang());

    }


    public void run() {
        long startTime_img = System.currentTimeMillis();

        try {
            long startTime;
            long endTime;

            incrementStartedCounter();
            logger.info("Start processing " + (startedCounter) + " | title: " + original_title);

            // Skip non photo file
            if (isNotPhoto(original_title)) {
                return;
            }

            isFlickr = false;
            isPanoramio = false;
            boolean needToMatchTitle = true;

            startTime = System.currentTimeMillis();
            MediaWikiCommonsAPI.CommonsMetadata commonsMetadata = mediaWikiCommonsAPI.createMetadata(original_title);
            endTime = System.currentTimeMillis();
            logger.debug("Execution time for mediaWikiCommonsAPI.createMetadata(): " + (endTime - startTime));
            time_mediaWikipeida.addValue((endTime - startTime));

            //Filter out non-topical categories
            startTime = System.currentTimeMillis();
            preprocessCommonsMetadata(commonsMetadata);
            endTime = System.currentTimeMillis();
            logger.debug("Execution time for preprocessCommonsMetadata(): " + (endTime - startTime));
            time_preprocessCommonsMetadata.addValue((endTime - startTime));

            Set<String> allYagoEntities = new HashSet<>();
            String yago_match = null;

            //Parse the Categories
            for (String category: commonsMetadata.getCategories()) {
                startTime = System.currentTimeMillis();
                try {
                    if (category != null && !category.isEmpty()) {
                        // Translate
                        ProcessSingleImageRunnable.TranslationResults translationResults = translateToEnglish(category);

                        // Match normally
                        yago_match = matchNounPhraseTranslation2Yago(translationResults);

                        // Print results
                        if (yago_match != null) {
                            // switch the needToMatchTitle flag
                            needToMatchTitle = false;

                            //prepare data to print to per_img txt
                            if (!allYagoEntities.contains(yago_match)){
                                // print to per_tag txt
                                appendLinetoFile(commonsMetadata.getPageID() + "\t" + original_title + "\t" + yago_match, "./output_per_tag.tsv");

                                // print to std out
                                logger.debug(commonsMetadata.getPageID() + "\t" + original_title + "\t" + yago_match);

                                // add the categories to yago_match
                                allYagoEntities.add(yago_match);
                            }
                        }
                    }
                } catch (Exception exception) {
                    logger.error("Error when parsing file: " + original_title);
                    logger.error("Error when parsing category: " + category);
                    logger.error(exception.getStackTrace());
                }
                endTime = System.currentTimeMillis();
                logger.debug("Execution time to process one category: " + (endTime - startTime));
                time_processOneCategory.addValue((endTime - startTime));
            }

            // Nulify yago_match because we will always try to match descriptions.
            yago_match = null;
            startTime = System.currentTimeMillis();
            try {
                // If the description exist try to match it
                if (commonsMetadata.getDescription() != null && !commonsMetadata.getDescription().isEmpty()) {
                    String strDescription = commonsMetadata.getDescription();

                    // Translate for the first phrase
                    ProcessSingleImageRunnable.TranslationResults firstPhraseTranslation = getFirstPhraseTranslationResults(translateToEnglish(strDescription));
                    String firstPhraseEng = firstPhraseTranslation.getTranslatedText();

                    // if the first phrase is short enough, match
                    if (!firstPhraseEng.isEmpty() && firstPhraseEng.split("_").length < MAXTOKENSINAPHRASE && isValidNounGroup(firstPhraseEng)) {
                        yago_match = matchNounPhraseTranslation2Yago(firstPhraseTranslation);
                    }

                    // Check and print
                    if (yago_match != null) {
                        // switch the needToMatchTitle flag
                        needToMatchTitle = false;

                        //prepare data to print to per_img txt
                        if (!allYagoEntities.contains(yago_match)){
                            // print to per_tag txt
                            appendLinetoFile(commonsMetadata.getPageID() + "\t" + original_title + "\t" + yago_match, "./output_per_tag.tsv");

                            // print to std out
                            logger.debug(commonsMetadata.getPageID() + "\t" + original_title + "\t" + yago_match);

                            // add the categories to yago_match
                            allYagoEntities.add(yago_match);
                        }
                    }
                }
            } catch (Exception exception) {
                logger.error("Error when parsing file: " + original_title);
                logger.error("Error when parsing description: " + commonsMetadata.getDescription());
                logger.error(exception.getStackTrace());
            }
            endTime = System.currentTimeMillis();
            logger.debug("Execution time to process one description " + (endTime - startTime));
            time_processOneDescription.addValue((endTime - startTime));


            // Start to match the title
            try {
                // If no thing is matched so far, try to match the original_title
                if (yago_match == null && needToMatchTitle) {
                    String proper_title = commonsMetadata.getTitle();

                    if (isNotBlacklist(proper_title) && proper_title.length() > MINCHARSINTITLE) {
                        // Translate
                        ProcessSingleImageRunnable.TranslationResults translationResults = translateToEnglish(proper_title);

                        // Match normally
                        // Using the title2class routine
                        // pro e.g.:
                        //      https://commons.wikimedia.org/wiki/File:ArtAndFeminism_2017-Puerto_Rico25.jpg
                        //      https://commons.wikimedia.org/wiki/File:Young_swan_alone_115810909.jpg
                        // con e.g.:
                        yago_match = this.matchCategory.title2class(translationResults.getTranslatedText());

                        if (yago_match != null) {
                            //prepare data to print to per_img txt
                            if (!allYagoEntities.contains(yago_match)){
                                // print to per_tag txt
                                appendLinetoFile(commonsMetadata.getPageID() + "\t" + original_title + "\t" + yago_match, "./output_per_tag.tsv");

                                // print to std out
                                logger.debug(commonsMetadata.getPageID() + "\t" + original_title + "\t" + yago_match);

                                // add the categories to yago_match
                                allYagoEntities.add(yago_match);
                            }
                        }
                    }
                }
            } catch (Exception exception) {
                logger.error("Error when parsing file: " + original_title);
                logger.error("Error when parsing title: " + commonsMetadata.getTitle());
                logger.error(exception.getStackTrace());
            }


            // Increment Flickr and Panoramio counter
            if (isFlickr) {incrementFlickrCounter();}
            if (isPanoramio) {incrementPanoramioCounter();}

            appendLinetoFile(commonsMetadata.getPageID() + "\t" + original_title + "\t" + allYagoEntities.toString(),"./output_per_img.tsv");
        } finally {
            long endTime_img = System.currentTimeMillis();
            time_oneImage.addValue((endTime_img - startTime_img));
            ProcessSingleImageRunnable.incrementCompletedCounter();
        }
    }
}
