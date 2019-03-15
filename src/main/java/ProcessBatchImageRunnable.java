import basics.FactComponent;
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
import java.util.concurrent.ConcurrentHashMap;

public class ProcessBatchImageRunnable implements Runnable {
    /**
     * Getters and Setters
     */

    public static void setPreferredMeanings(Map<String, String> preferredMeanings) {
        ProcessBatchImageRunnable.preferredMeanings = preferredMeanings;
    }

    public static void setNonConceptualCategories(Set<String> nonConceptualCategories) {
        ProcessBatchImageRunnable.nonConceptualCategories = nonConceptualCategories;
    }

    public static void setYagoConnection(Connection yagoConnection) {
        ProcessBatchImageRunnable.yagoConnection = yagoConnection;
    }

    public static void setYagoLowercase2Original(HashMap<String, HashSet<String>> yagoLowercase2Original) {
        ProcessBatchImageRunnable.yagoLowercase2Original = yagoLowercase2Original;
    }

    public static void setYagoOriginal2Type(HashMap<String, HashSet<String>> yagoOriginal2Type) {
        ProcessBatchImageRunnable.yagoOriginal2Type = yagoOriginal2Type;
    }

    public static void setcachedParentCategories(ConcurrentHashMap<String, List<String>> cachedParentCategories) {
        ProcessBatchImageRunnable.cachedParentCategories = cachedParentCategories;
    }

    public static void setlinuxEnglishWord(HashSet<String> linuxEnglishWord) {
        ProcessBatchImageRunnable.linuxEnglishWord = linuxEnglishWord;
    }

    public static int getCompletedCounter() {
        return completedCounter;
    }

    public static int getFailedImageCounter(){
        return failedImageCounter;
    }

    private synchronized static void incrementCompletedCounter() {
        completedCounter++;
    }

    private synchronized static void incrementFailedImageCounter(){
        failedImageCounter++;
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

    private synchronized static void incrementValidPhotoCounter() {validPhotoCounter++;}

    private synchronized static void addToChartoTranslateCounter(long numOfCharToAdd) {ChartoTranslateCounter += numOfCharToAdd;}

    public static int getFlickrCounter(){
        return FlickrCounter;
    }

    public static int getPanoramioCounter(){
        return PanoramioCounter;
    }

    public static int getValidPhotoCounter() {return validPhotoCounter;}

    /** Holds the preferred meanings */
    private static Map<String, String> preferredMeanings;

    // Holds the nonconceptual categorie
    private static Set<String> nonConceptualCategories;

    private static Connection yagoConnection;

    private static HashMap<String, HashSet<String>> yagoLowercase2Original;

    private static HashMap<String, HashSet<String>> yagoOriginal2Type;

    private static ConcurrentHashMap<String, List<String>> cachedParentCategories;

    private static HashSet<String> linuxEnglishWord;

    private static final Logger logger = LogManager.getLogger(ProcessBatchImageRunnable.class);

    private static int completedCounter = 0;

    private static int startedCounter = 0;

    private static int validPhotoCounter = 0;

    private static final int MAXTOKENSINAPHRASE = 11;

    private static final int MINCHARSINTITLE= 5;

    private static int FlickrCounter = 0;

    private static int PanoramioCounter = 0;

    static long ChartoTranslateCounter = 0;

    private static LanguageDetector languageDetector;

    private static TextObjectFactory textObjectFactory;

    private static final Object translationLock = new Object();

    private static int failedImageCounter = 0;

    // DEBUG: for debug purpose only
    static final SummaryStatistics time_mediaWikipeida = new SynchronizedSummaryStatistics();

    static final SummaryStatistics time_preprocessCommonsMetadata = new SynchronizedSummaryStatistics();

    static final SummaryStatistics time_processOneCategory = new SynchronizedSummaryStatistics();

    static final SummaryStatistics time_processOneDescription = new SynchronizedSummaryStatistics();

    static final SummaryStatistics time_oneBatch = new SynchronizedSummaryStatistics();

    static final ConcurrentHashMap<String, String> translationCache = new ConcurrentHashMap<>();

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

    private Translator translateAPI;

    private ArrayList<String> originalImgCatsArray;

    private boolean isFlickr = false;

    private boolean isPanoramio = false;

    private MatchCategory matchCategory;


    private static boolean isNotImages(String fileName) {
        return (fileName.contains(".webm") || fileName.contains(".ogg") || fileName.contains(".mp3") ||
                fileName.contains(".midi") || fileName.contains(".wave") || fileName.contains(".flac"));
    }

    private static final String[] BLACKLIST_STARTWITH_CATEGORIES = { "commons", "cc-", "pd_", "categories_", "items_with", "attribution_", "gfdl", "pd-", "file_", "files_",
            "photos_of_", "photos,_created_", "media_missing_", "projet_québec", "work_", "scans_", "scan_", "pcl", "images_", "image_", "gpl", "wiki", "glam_", "fop-", "fop_",
            "location_", "executive_office_of_the_president", "with_trademark", "hidden_categories", "godl"
    };

    private static final String[] BLACKLIST_CONTAINS_CATEGORIES ={"copyright", "license", "media_type", "file_format", "media_needing", "flickr", "self-published_work", "by_user",
            "_images", "_image", "panorami", "photos_by", "upload", "personality_rights_warning", "media_lacking", "'s_photo", "taken_by", "_deaths", "unsupported_object",
            "media_supported_by", "media_by", "media_from", "media_with", "pages_with_map", "media_contributed_by", "user:", "created_with", "(given_name)", "_births", "(surname)",
            "photograph", "wikidata", "taken_with", "robert_d._ward", "nike_specific_patterns", "template_unknown", "_temp_", "department_of_", "supported_by_", "wlm",
            "_files_", "_file_", "lgpl", "protected_", "wikipedia", "photos_from", "media_donated_by", "nature_neighbors", "_location",  "photos,_created_by_", "project_",
            "djvu_", "gerard_dukker", "wikimania", "translation_possible", "attribute_", "wikiafrica_", "_view_", "_views_", "_wikimedia_event", "_tamilwiki", "media_created_by",
            "rogers_fund", "serie",
            "elef_milim", "_work_", "_scan_", "_by_raboe", "available", "_version", "_applicable", "possible", "featured_pictures", "svg", "nypl", "_wiki_", "anonymous", "categories"
    };

    private static final String[] BLACKLIST_EQUAL_CATEGORIES={"fal", "attribution", "retouched_pictures", "vector_graphics", "cecill"
    };


    ProcessBatchImageRunnable(ArrayList<String> originalImageCatsArray) {
        this.originalImgCatsArray = originalImageCatsArray;

        try {
            // Set up the MediaWikiCommons API
            this.mediaWikiCommonsAPI = new MediaWikiCommonsAPI();

            // Use local language detector
            if (TagstoYagoMatcher.getPROPERTIES().getProperty("useLocalLangDetector").equals("true")) {
                //load the static language detector if not already:
                if (languageDetector == null && textObjectFactory == null) {
                    List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();

                    //build language detector:
                    languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                            .withProfiles(languageProfiles)
                            .build();

                    //create a text object factory
                    textObjectFactory = CommonTextObjectFactories.forDetectingShortCleanText();
                }

            }
        } catch (Exception exception) {
            logger.error(exception.getStackTrace());
        }

        if (TagstoYagoMatcher.getPROPERTIES().getProperty("LoadYago2Memory").equals("true")) {
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

    private List<String> preprocessCommonsCategories(List<String> commonsCategories) {
        // preprocess categories
        ArrayList<String> ultimate_category = new ArrayList<>();
        ArrayList<String> additional_category = new ArrayList<>();

        for (String current_category: commonsCategories) {
            //Split the category with slash e.g.  Football kit body/Nike specific patterns. Only use the first half
            if (current_category.contains("/") && current_category.length() > 1) {
                additional_category.add(current_category.split("/")[0]);
                continue;
            }

            //Split the category with (space)dash(space)
            if (current_category.contains("_-_")){
                List<String> splitByDash = Arrays.asList(current_category.split("_-_"));

                // If too many dashes, just add the first part
                if (splitByDash.size() > 2) {
                    additional_category.add(splitByDash.get(0));
                } else {
                    // If the second part is too short, just add the first part
                    if (splitByDash.get(1).length() < 3) {
                        additional_category.add(splitByDash.get(0));
                    } else {
                        additional_category.addAll(splitByDash);
                    }
                }

                continue;
            }

            // Deal with special case "Periodic table positions"
            if (current_category.contains("Periodic_table_positions")) {
                current_category = "Periodic_table";
            }

            // Deal with special case "general view of Casa Mila"
            if (current_category.toLowerCase().contains("views_of_")) {
                current_category = current_category.substring(current_category.indexOf("views_of_")+9);
            }
            if (current_category.toLowerCase().contains("view_of_")) {
                current_category = current_category.substring(current_category.indexOf("view_of_")+8);
            }

            // Parse the "exterior of " and "interior of"
            if (current_category.toLowerCase().contains("exterior_of") ||
                    current_category.toLowerCase().contains("exteriors_of") ||
                    current_category.toLowerCase().contains("interior_of") ||
                    current_category.toLowerCase().contains("interiors_of")) {
                additional_category.addAll(Arrays.asList(current_category.split("_of_")));
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

        return ultimate_category;
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

        // Preprocess categories
        metadata.setCategories(preprocessCommonsCategories(metadata.getCategories()));


        // preprocess descriptions
        // remove substring inside parenthesis since it cause problem with CoreNLP parsing results.
        // defer calling isValidNounGroup to later since parsing the first phrase of description needs Google Translation.
        metadata.setDescription(metadata.getDescription().replaceAll("\\s*\\([^\\)]*\\)\\s*", " ").trim());


        // preprocess title
        metadata.setTitle(preprocessTitle(metadata.getOriginalTitle()));

    }

    private boolean needHardCodetoEN(String langCode) {
        return langCode.equals("an") || langCode.equals("ast") || langCode.equals("br");
    }

    private boolean lotsOfForeignWords(String strip_original) {
        String [] tokens = strip_original.toLowerCase().replaceAll("\\p{Punct}", "").split(" ");
        int numEnglishToken = 0;

        for (String token: tokens) {
            if (ProcessBatchImageRunnable.linuxEnglishWord.contains(token)) {
                numEnglishToken++;
            }
        }

        return (numEnglishToken < (tokens.length / 2));
    }

    private TranslationResults translateToEnglish(String original_text){
        String strip_original = FactComponent.stripCat(original_text).trim();

        String englishText = original_text;

        String lang = "en";



        if (TagstoYagoMatcher.getPROPERTIES().getProperty("useTranslator").equals("true")){
            // Initialize the translator api
            switch (TagstoYagoMatcher.getPROPERTIES().getProperty("TranslationAPI")) {
                case "microsoft": {
                    this.translateAPI = new MicrosoftTranslatorAPI();
                    break;
                }
                case "google_free": {
                    this.translateAPI = new GoogleFreeTranslateAPI();
                    break;
                }
            }


            // Use local language detector
            if (TagstoYagoMatcher.getPROPERTIES().getProperty("useLocalLangDetector").equals("true")) {

                // Only translate if all tokens are in foreign languages.
                if (lotsOfForeignWords(strip_original)) {
                    TextObject textObject = textObjectFactory.forText(strip_original);
                    Optional<LdLocale> langOptional = languageDetector.detect(textObject);
                    lang = langOptional.isPresent()?langOptional.get().getLanguage():"en";

                    // translate oc to fr
                    lang = lang.equals("oc")?"fr":lang;

                    // translate br to en
                    if (needHardCodetoEN(lang)) {
                        lang = "en";
                    }
                }

            } else {
                try {
                    lang = translateAPI.detect(strip_original);
                } catch (TranslateException exception) {
                    lang = "en";
                }
            }

            // Translate the text if not in English
            if (! lang.equals("en")) {
                logger.info("Need translation:" + original_text);

                String translationCachedResult;

                // get from cache
                translationCachedResult = ProcessBatchImageRunnable.translationCache.get(strip_original);

                // If the orginal text has been cached
                if (translationCachedResult != null) {
                    englishText = translationCachedResult;
                } else {
                    // If not cached, use the translation api
                    englishText = translateAPI.translate(strip_original, lang, "en");
                    if (englishText.equals(strip_original)) {
                        lang = "en";
                    }

                    // Synchronize the put operation
                    ProcessBatchImageRunnable.translationCache.put(strip_original, englishText);

                    // Add this to counter
                    addToChartoTranslateCounter(strip_original.length());
                }
            }
        }


        return (new TranslationResults(original_text, englishText, lang));
    }

    private String matchNounPhraseTranslation2Yago (TranslationResults translationResults) {
        String yago_match;

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
        String firstPhraseEng;

        if (translation.getTranslatedText().equals("")) {
            firstPhraseEng = "";
        } else {
            firstPhraseEng = NLPUtils.getFirstPhrase(translation.getTranslatedText()).trim().replaceAll(" ","_");
        }

        String firstPhraseOriginal = NLPUtils.getFirstPhrase(translation.getOriginalText()).trim().replaceAll(" ", "_");

        return new TranslationResults(firstPhraseOriginal, firstPhraseEng, translation.getLang());

    }

    private TranslationResults translateDescription(String original_description) {
        String strip_original = FactComponent.stripCat(original_description).trim();
        strip_original = strip_original.startsWith("\n")?strip_original.substring("\n".length()):strip_original;
        // Just use the first paragraph in case the text is a combination of English and foreign language
        // e.g.  description in https://commons.wikimedia.org/wiki/File:Matereialseilbahn_Dotternhausen_22022014.JPG
        if (strip_original.contains("\n")) {
            strip_original = strip_original.split("\n")[0].trim();
        }

        String lang;
        // Synchronized this block since it uses static methods and variables
        synchronized (translationLock) {
            TextObject textObject = textObjectFactory.forText(strip_original);
            Optional<LdLocale> langOptional = languageDetector.detect(textObject);
            lang = langOptional.isPresent()?langOptional.get().getLanguage():"en";
        }

        // translate oc to fr
        lang = lang.equals("oc")?"fr":lang;

        // translate br to en
        if (needHardCodetoEN(lang)) {
            lang = "en";
        }

        String englishText = strip_original;

        //DEBUG: for NonEnglish one, still use the English text
        if (!lang.equals("en")) {
            englishText = strip_original;
        }

        return (new TranslationResults(original_description, englishText, "en"));
    }


    /**
     * Match all the categories
     * @param commonsMetadata SE
     * @param allYagoEntities SE
     * @param allMatchingResults SE
     * @param allOriginalCategories Se
     */
    private void matchAllCategories(MediaWikiCommonsAPI.CommonsMetadata commonsMetadata,
                                    Set<String> allYagoEntities,
                                    List<String> allMatchingResults,
                                    List<String> allOriginalCategories){
        long startTime;
        long endTime;
        String yago_match;


        //Parse the Categories
        for (String category: commonsMetadata.getCategories()) {
            allOriginalCategories.add("<"+category+">");
            startTime = System.currentTimeMillis();
            try {
                if (category != null && !category.isEmpty()) {
                    // Translate
                    ProcessBatchImageRunnable.TranslationResults translationResults = translateToEnglish(category);

                    // Match normally
                    yago_match = matchNounPhraseTranslation2Yago(translationResults);

                    // Print results
                    if (yago_match != null) {

                        //prepare data to print to per_img txt
                        allMatchingResults.add(yago_match);
                        if (!allYagoEntities.contains(yago_match)){
                            // print to per_tag txt
                            appendLinetoFile(commonsMetadata.getPageID() + "\t" + commonsMetadata.getOriginalTitle() + "\t" + yago_match, "./output_per_tag.tsv");

                            // add the categories to yago_match
                            allYagoEntities.add(yago_match);
                        }
                    } else {
                        //Search parent categories
                        List<String> parentCategories;
                        if (cachedParentCategories.get(category) != null) {
                            // First search in the cache
                            parentCategories = cachedParentCategories.get(category);
                        } else {
                            // If not found, query the WM API and save the results
                            parentCategories = MediaWikiCommonsAPI.getParentCategories(category);
                            parentCategories = preprocessCommonsCategories(parentCategories);
                            cachedParentCategories.put(category, parentCategories);
                        }

                        List<String> parentMatchingResults = new ArrayList<>();

                        for (String parentCategory: parentCategories) {
                            // Translate
                            translationResults = translateToEnglish(parentCategory);

                            // Match normally
                            yago_match = matchNounPhraseTranslation2Yago(translationResults);

                            // Print results
                            if (yago_match != null) {

                                //prepare data to print to per_img txt
                                if (!allYagoEntities.contains(yago_match)){
                                    // print to per_tag txt
                                    appendLinetoFile(commonsMetadata.getPageID() + "\t" + commonsMetadata.getOriginalTitle() + "\t" + yago_match, "./output_per_tag.tsv");

                                    // add the categories to yago_match
                                    allYagoEntities.add(yago_match);

                                    parentMatchingResults.add(yago_match);
                                }
                            }
                        }

                        if (!parentMatchingResults.isEmpty()) {
                            allMatchingResults.add("<"+parentMatchingResults.toString().replace("<","{").replace(">","}")+">");
                        } else {
                            allMatchingResults.add("<>");
                        }


                    }
                }
            } catch (Exception exception) {
                logger.error("Error when parsing file: " + commonsMetadata.getOriginalTitle());
                logger.error("Error when parsing category: " + category);
                logger.error(exception.getStackTrace());
            }



            endTime = System.currentTimeMillis();
            //logger.debug("Execution time to process one category: " + (endTime - startTime));
            time_processOneCategory.addValue((endTime - startTime));
        }
    }

    private void matchDescription(MediaWikiCommonsAPI.CommonsMetadata commonsMetadata,
                                  Set<String> allYagoEntities,
                                  List<String> allMatchingResults,
                                  List<String> allOriginalCategories){
        long startTime;
        long endTime;
        String yago_match = null;


        //If needed, match the description
        startTime = System.currentTimeMillis();
        try {
            // If the description exist try to match it
            if (commonsMetadata.getDescription() != null && !commonsMetadata.getDescription().isEmpty()) {
                String strDescription = commonsMetadata.getDescription();

                // Translate for the first phrase
                //TranslationResults firstPhraseTranslation = getFirstPhraseTranslationResults(translate(strDescription));
                TranslationResults firstPhraseTranslation = getFirstPhraseTranslationResults(translateDescription(strDescription));
                String firstPhraseEng = firstPhraseTranslation.getTranslatedText();

                // if the first phrase is short enough, match
                if (!firstPhraseEng.isEmpty() && firstPhraseEng.split("_").length < MAXTOKENSINAPHRASE && isValidNounGroup(firstPhraseEng)) {
                    yago_match = matchNounPhraseTranslation2Yago(firstPhraseTranslation);
                }

                // Check and print
                if (yago_match != null) {
                    //prepare data to print to per_img txt
                    if (!allYagoEntities.contains(yago_match)){
                        // we only save them if the description matched
                        allOriginalCategories.add("<Descriptions_omitted>");
                        allMatchingResults.add(yago_match);


                        // print to per_tag txt
                        appendLinetoFile(commonsMetadata.getPageID() + "\t" + commonsMetadata.getOriginalTitle() + "\t" + yago_match, "./output_per_tag.tsv");

                        // add the categories to yago_match
                        allYagoEntities.add(yago_match);
                    }
                }
            }
        } catch (Exception exception) {
            logger.error("Error when parsing file: " + commonsMetadata.getOriginalTitle());
            logger.error("Error when parsing description: " + commonsMetadata.getDescription());
            logger.error(exception.getStackTrace());
        }
        endTime = System.currentTimeMillis();
        //logger.debug("Execution time to process one description " + (endTime - startTime));
        time_processOneDescription.addValue((endTime - startTime));

    }


    public void run() {
        long startTime_batch = System.currentTimeMillis();

        try {
            long startTime;
            long endTime;

            // Retrieve all the metadata
//            startTime = System.currentTimeMillis();
//            List<MediaWikiCommonsAPI.CommonsMetadata> commonsMetadataList = mediaWikiCommonsAPI.createMetadata(this.originalImgCatsArray);
//            endTime = System.currentTimeMillis();
//            time_mediaWikipeida.addValue((endTime - startTime));

            List<MediaWikiCommonsAPI.CommonsMetadata> commonsMetadataList = mediaWikiCommonsAPI.createMetadataFromDump(this.originalImgCatsArray);

            for (MediaWikiCommonsAPI.CommonsMetadata commonsMetadata: commonsMetadataList) {
                // threadsafe increment the started Counter
                incrementStartedCounter();

                // If failed to parse JSON for this object, skip but increment the failure counter
                if (commonsMetadata.getOriginalTitle() == null) {
                    incrementFailedImageCounter();
                    continue;
                }

                String original_title = commonsMetadata.getTitle();

                try {

                    // Flag for flickr and panoramio counter
                    isFlickr = false;
                    isPanoramio = false;

                    //Filter out non-topical categories
                    startTime = System.currentTimeMillis();
                    preprocessCommonsMetadata(commonsMetadata);
                    endTime = System.currentTimeMillis();
                    time_preprocessCommonsMetadata.addValue((endTime - startTime));

                    Set<String> allYagoEntities = new HashSet<>();
                    String yago_match;

                    //Record the matching_results
                    List<String> allMatchingResults = new ArrayList<>();
                    List<String> allOriginalCategories = new ArrayList<>();

                    // Match all categories
                    matchAllCategories(commonsMetadata, allYagoEntities, allMatchingResults, allOriginalCategories);


                    // If needed, match descriptions
                    if (allYagoEntities.isEmpty()) {
                        matchDescription(commonsMetadata, allYagoEntities, allMatchingResults, allOriginalCategories);
                    }


                    // Save categories info
                    // print to ground truth txt
//                    String contentMatchingResults = String.join(", ", allMatchingResults);
//                    String contentOriginalCategories = String.join(", ", allOriginalCategories);
//                    appendLinetoFile(commonsMetadata.getPageID() + "\t" + original_title + "\t" + contentOriginalCategories + "\t" + contentMatchingResults,"./output_groundtruth.tsv");

                    // Start to match the title
                    try {
                        // If no thing is matched so far, try to match the title
                        if (allYagoEntities.isEmpty()) {
                            String proper_title = commonsMetadata.getTitle();

                            if (isNotBlacklist(proper_title) && proper_title.length() > MINCHARSINTITLE) {
                                // Translate
                                ProcessBatchImageRunnable.TranslationResults translationResults = translateToEnglish(proper_title);

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

                                        // save the categories to yago_match
                                        allYagoEntities.add(yago_match);
                                    }
                                }
                            }
                        }
                    } catch (Exception exception) {
                        logger.error("Error when parsing file: " + original_title);
                        logger.error("Error when parsing title: " + commonsMetadata.getOriginalTitle());
                        logger.error(exception.getStackTrace());
                    }


                    // Increment Flickr and Panoramio counter
                    if (isFlickr) {incrementFlickrCounter();}
                    if (isPanoramio) {incrementPanoramioCounter();}

                    incrementValidPhotoCounter();
                    appendLinetoFile(commonsMetadata.getPageID() + "\t" + original_title + "\t" + allYagoEntities.toString(),"./output_per_img.tsv");

                    if (startedCounter % 10000 == 0) {
                        logger.info("Finished processing " + (startedCounter) + " | title: " + commonsMetadata.getOriginalTitle());
                    }
                } finally {
                    ProcessBatchImageRunnable.incrementCompletedCounter();
                }
            }
        } finally {
            long endTime_batch = System.currentTimeMillis();
            time_oneBatch.addValue((endTime_batch - startTime_batch));
            logger.debug("Execution time to process one batch of images: " + (endTime_batch - startTime_batch));
        }


    }
}
