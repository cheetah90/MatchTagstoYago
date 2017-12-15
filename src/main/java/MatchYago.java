import basics.FactComponent;
import javatools.administrative.Parameters;
import fromOtherSources.PatternHardExtractor;
import fromOtherSources.WordnetExtractor;
import utils.Theme;
import utils.NLPUtils;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.cloud.translate.Translate.TranslateOption;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.translate.*;

public class MatchYago {

    private static final Properties PROPERTIES = new Properties();

    private boolean isFlickr = false;

    private boolean isPanoramio = false;

    private static final Logger logger = LogManager.getLogger(MatchYago.class);

    private static long FlickrCounter = 0;

    private static long PanoramioCounter = 0;

    private static final int MAXTOKENSINAPHRASE = 11;

    private static final int MINCHARSINTITLE= 5;

    private String[] BLACKLIST_STARTWITH_CATEGORIES = { "commons", "cc-", "pd_", "categories_", "items_with", "attribution_", "gfdl", "pd-", "files_using_", "files_",
            "photos_of_", "photos,_created_", "media_missing_", "projet_québec", "work_", "scans_", "scan_"};

    private String[] BLACKLIST_CONTAINS_CATEGORIES ={"copyright", "license", "media_type", "file_format", "media_needing", "flickr", "self-published_work", "images_by", "by_user",
            "images_from", "panorami", "images_in", "photos_by", "images_by", "upload", "geograph_images", "personality_rights_warning", "media_lacking",
            "media_supported_by", "media_by", "media_from", "media_with", "files_from", "pages_with_map", "media_contributed_by", "user:", "files_created_by",
            "photograph", "wikidata", "taken_with", "robert_d._ward", "nike_specific_patterns", "files_with_no", "template_unknown", "_temp_", "department_of_", "supported_by_",
            "images_with_", "files_by", "lgpl", "protected_", "wikipedia", "photos_from", "media_donated_by", "nature_neighbors", "_locations_", "photos,_created_by_", "project_",
            "djvu_files", "images_of_", "gerard_dukker", "wikimania", "translation_possible", "attribute_", "image_description", "wikiafrica_", "_view_", "_views_",
            "elef_milim", "_work_"
    };

    private String[] BLACKLIST_EQUAL_CATEGORIES={"fal", "attribution", "retouched_pictures"
    };

    private Connection db4SamplesConnection;

    private Connection yagoConnection;

    private Translate googleTranslate;

    private MatchCategory matchCategory;

    private MediaWikiCommonsAPI mediaWikiCommonsAPI;

    private final static HashMap<String, HashSet<String>> yagoLowercase2Original = new HashMap<>();

    private final static HashMap<String, HashSet<String>> yagoOriginal2Type = new HashMap<>();

    /** Holds the preferred meanings */
    protected Map<String, String> preferredMeanings;

    class TranslationResults {
        public String getOriginalText() {
            return originalText;
        }

        public String getTranslatedText() {
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

    public MatchYago(){
        // Holds the nonconceptual categories
        Set<String> nonConceptualCategories = new HashSet<>();

        // Initialize MediaWikiCommonsAPI
        this.mediaWikiCommonsAPI = new MediaWikiCommonsAPI();


        try {
            // necessary load for yago
            nativeYagoLoad();

            //Load properties file
            PROPERTIES.load(new FileInputStream("./src/main/resources/config.properties"));

            // Load the samples
            this.db4SamplesConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Samples.port")+"/"+PROPERTIES.getProperty("db4Samples.name"),
                    PROPERTIES.getProperty("db4Samples.username"), PROPERTIES.getProperty("db4Samples.password"));


            // Set up the Google Translate API connection
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("./src/main/resources/wikicommons-1c391c623d29.json"));
            this.googleTranslate = TranslateOptions.newBuilder().setCredentials(credentials).build().getService();

            // Initialize hardcoded mappings
            nonConceptualCategories = PatternHardExtractor.CATEGORYPATTERNS.factCollection().seekStringsOfType("<_yagoNonConceptualWord>");
            preferredMeanings = WordnetExtractor.PREFMEANINGS.factCollection().getPreferredMeanings();
        } catch (IOException | SQLException  e) {
            e.printStackTrace();
        }

        logger.info("Start to load Yago data...");
        // Initialize connection to Yago's sample
        if (PROPERTIES.getProperty("LoadYago2Memory").equals("true")) {
            logger.info("Choose to load Yago data into memory!");
            loadYagotoMemory();
            this.matchCategory = new MatchCategory(this.preferredMeanings, nonConceptualCategories, yagoLowercase2Original, yagoOriginal2Type);
        } else {
            logger.info("Choose to load Yago data from a database!");
            try {
                Connection yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4Yago.name"),
                        PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

                this.matchCategory = new MatchCategory(this.preferredMeanings, nonConceptualCategories, yagoConnection);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

    }

    private TranslationResults translateToEnglish(String original_text){
        String strip_original = FactComponent.stripCat(original_text);
        // Just use the first paragraph in case the text is a combination of English and foreign language
        // e.g.  description in https://commons.wikimedia.org/wiki/File:Matereialseilbahn_Dotternhausen_22022014.JPG
        if (strip_original.contains("\n")) {
            strip_original = strip_original.split("\n")[0];
        }
        String englishText = original_text;

        Detection detection = this.googleTranslate.detect(strip_original);
        String lang = detection.getLanguage();

        try {
            if (! lang.equals("en")) {
                Translation translation = this.googleTranslate.translate(
                        strip_original,
                        TranslateOption.sourceLanguage(lang),
                        TranslateOption.targetLanguage("en"));

                englishText = translation.getTranslatedText();
            }
        } catch (TranslateException exception) {
            logger.error("Something wrong with the Google Translate API");
            englishText = "";
        }

        return (new TranslationResults(original_text, englishText, lang));
    }


    private List<String >queryAllPageTitles() throws SQLException{
        ArrayList<String> pageTitles = new ArrayList<>();

        String query = PROPERTIES.getProperty("query4Samples");
        Statement stmt = db4SamplesConnection.createStatement();
        ResultSet rs_title = stmt.executeQuery(query);

        while (rs_title.next()){
            pageTitles.add(rs_title.getString("file_title"));
        }

        return pageTitles;
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


    private void clearOutputfile(String outputFileName){
        try {
            Writer output = new BufferedWriter(new FileWriter(outputFileName, false));
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
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
        // update Flickr and Panoramio counter accordingly TODO: this need to ba thread safe implementation
        if (current_category.toLowerCase().contains("flickr")) {
            isFlickr = true;
        }

        if (current_category.toLowerCase().contains("panoramio")) {
            isPanoramio = true;
        }

        //This is a hack to deal with whitelist
        if (isWhiltelist(current_category)){
            return true;
        } else if (isNotBlacklist(current_category)) {
            return true;
        }

        return false;
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

    private TranslationResults getFirstPhraseTranslationResults(TranslationResults translation) {
        String firstPhraseEng = NLPUtils.getFirstPhrase(translation.getTranslatedText()).trim().replaceAll(" ","_");
        String firstPhraseOriginal = NLPUtils.getFirstPhrase(translation.getOriginalText()).trim().replaceAll(" ", "_");

        return new TranslationResults(firstPhraseOriginal, firstPhraseEng, translation.getLang());

    }

    private String matchNounPhraseTranslation2Yago (TranslationResults translationResults) {
        String yago_match = null;

        // To deal with foreign proper noun, direct match lang_code + original_text
        if (!translationResults.getLang().equals("en")) {
            yago_match = this.matchCategory.directMatch(translationResults.getLang().toLowerCase() + "/" + translationResults.getOriginalText().toLowerCase());
            if (this.matchCategory.isValidYagoItem(yago_match, translationResults.getLang().toLowerCase() + "/" + translationResults.getOriginalText().toLowerCase())) {
                return yago_match;
            }

            // match the foreign text without lang code
            yago_match = this.matchCategory.directMatch(translationResults.getOriginalText().toLowerCase());
            if (this.matchCategory.isValidYagoItem(yago_match, translationResults.getOriginalText().toLowerCase())) {
                return yago_match;
            }
        }

        // Parse the English NounPhrase
        yago_match = this.matchCategory.matchNounGroup2Yago(translationResults.getTranslatedText());
        if (this.matchCategory.isValidYagoItem(yago_match, translationResults.getTranslatedText())){
            return yago_match;
        }

        return null;
    }

    private boolean isNotPhoto(String fileName) {
        return (fileName.contains(".webm") || fileName.contains(".ogg"));
    }

    boolean hadValidObject(String typeInfo) {
        return (!typeInfo.contains("wikicat_Abbreviations") && !typeInfo.contains("wordnet_first_name"));
    }

    private void loadResultSet(ResultSet rs) throws SQLException {
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            if (hadValidObject(object) ){

                // first add to yagoLowercase2Original
                if (yagoLowercase2Original.get(subject.toLowerCase()) == null) {
                    // the lowercase does not exist
                    HashSet<String> hashSet = new HashSet<>();
                    hashSet.add(subject);
                    yagoLowercase2Original.put(subject.toLowerCase(), hashSet);
                } else {
                    yagoLowercase2Original.get(subject.toLowerCase()).add(subject);
                }

                // Then add to yagoOriginal2Type
                if (yagoOriginal2Type.get(subject) == null) {
                    // the lowercase does not exist
                    HashSet<String> hashSet = new HashSet<>();
                    hashSet.add(object);
                    yagoOriginal2Type.put(subject, hashSet);
                } else {
                    yagoOriginal2Type.get(subject).add(object);
                }
            }

        }
    }

    private void loadYagotoMemory(){
        try {
            Connection yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4Yago.name"),
                    PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

            PreparedStatement stmt = null;

            logger.info("Start loading records from database");
            // load yagotypes
            String query_yagotype = "SELECT * FROM subset_yagotypes";
            stmt = yagoConnection.prepareStatement(query_yagotype);
            ResultSet rs = stmt.executeQuery();
            //Load the resultset
            loadResultSet(rs);
            rs.close();
            stmt.close();


            // Load the yagotaxonomy
            String query_yagotaxonomy = "SELECT * FROM YAGOTAXONOMY";
            stmt = yagoConnection.prepareStatement(query_yagotaxonomy);
            rs = stmt.executeQuery();
            loadResultSet(rs);
            rs.close();
            stmt.close();

            logger.info("Finish loading records from dataase");

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }


    private void startWorking(){
        //clearOutputfile
        clearOutputfile("./output_per_tag.tsv");
        clearOutputfile("./output_per_img.tsv");

        long counter = 0;

        List<String> pageTitles = null;

        try {
            pageTitles = queryAllPageTitles();
        } catch (SQLException exception) {
            exception.printStackTrace();
        }

        if (pageTitles == null) {
            return;
        }

        for (String original_title: pageTitles){
            // Skip non photo file
            if (isNotPhoto(original_title)) {
                continue;
            }


            counter ++;
            isFlickr = false;
            isPanoramio = false;
            boolean needToMatchTitle = true;


            logger.info("Start processing " + counter + " | title: " + original_title);

            MediaWikiCommonsAPI.CommonsMetadata commonsMetadata = this.mediaWikiCommonsAPI.createMeatadata(original_title);
            //Filter out non-topical categories
            preprocessCommonsMetadata(commonsMetadata);

            Set<String> allYagoEntities = new HashSet<>();
            String yago_match = null;

            //Parse the Categories
            for (String category: commonsMetadata.getCategories()) {
                try {
                    if (category != null && !category.isEmpty()) {
                        // Translate
                        TranslationResults translationResults = translateToEnglish(category);

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
                                logger.info("\t" + yago_match);

                                // add the categories to yago_match
                                allYagoEntities.add(yago_match);
                            }
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    logger.error("Error when parsing file: " + commonsMetadata.getTitle());
                    logger.error("Error when parsing category: " + category);
                }

            }

            // Nulify yago_match because we will always try to match descriptions.
            yago_match = null;

            try {
                // If the description exist try to match it
                if (commonsMetadata.getDescription() != null && !commonsMetadata.getDescription().isEmpty()) {
                    String strDescription = commonsMetadata.getDescription();

                    // Translate for the first phrase
                    TranslationResults firstPhraseTranslation = getFirstPhraseTranslationResults(translateToEnglish(strDescription));
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
                            logger.info("\t" + yago_match);

                            // add the categories to yago_match
                            allYagoEntities.add(yago_match);
                        }
                    }
                }
            } catch (Exception exception) {
                exception.printStackTrace();
                logger.error("Error when parsing file: " + commonsMetadata.getTitle());
                logger.error("Error when parsing description: " + commonsMetadata.getDescription());
            }


            try {
                // If no thing is matched so far, try to match the original_title
                if (yago_match == null && needToMatchTitle) {
                    String proper_title = commonsMetadata.getTitle();

                    if (isNotBlacklist(proper_title) && proper_title.length() > MINCHARSINTITLE) {
                        // Translate
                        TranslationResults translationResults = translateToEnglish(proper_title);

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
                                logger.info("\t" + yago_match);

                                // add the categories to yago_match
                                allYagoEntities.add(yago_match);
                            }
                        }
                    }
                }
            } catch (Exception exception) {
                exception.printStackTrace();
                logger.error("Error when parsing file: " + commonsMetadata.getTitle());
                logger.error("Error when parsing title: " + commonsMetadata.getTitle());
            }


            // Increment Flickr and Panoramio counter
            if (isFlickr) {FlickrCounter++;}
            if (isPanoramio) {PanoramioCounter++;}

            appendLinetoFile(commonsMetadata.getPageID() + "\t" + original_title + "\t" + allYagoEntities.toString(),"./output_per_img.tsv");

        }

    }

    /** Read file backwards. Does not use buffers, therefore slow */
    private static String tail(File src, int maxLines) throws IOException {
        if (src == null || !src.isFile()) return null;
        // based on https://stackoverflow.com/a/15612710
        StringBuilder sb = new StringBuilder();
        int lines = 0;
        try (RandomAccessFile f = new RandomAccessFile(src, "r")) {
            long length = f.length(), p;
            for (p = length - 1; p >= 0 && lines < maxLines; p--) {
                f.seek(p);
                int b = f.read();
                if (b == 10 || p == 0) {
                    if (p < length - 1) {
                        if (sb.length() > 0 && lines < maxLines - 1 && p != 0) sb.append('\n');
                        lines++;
                    }
                } else if (b != 13) {
                    sb.append((char) b);
                }
            }
        }

        return sb.reverse().toString();
    }

    private void nativeYagoLoad() throws IOException{
        String initFile = "./src/main/resources/yago.ini";
        Parameters.init(initFile);

        File outputFolder = Parameters.getFile("yagoFolder");

        // check which themes exist, and which we could reuse
        Set<Theme> available = new HashSet<>(), reusable = new HashSet<>();

        for (Theme t : Theme.all()) {
            File f = t.findFileInFolder(outputFolder);
            if (f == null || !tail(f, 1).contains("# end of file")) {
                continue;
            }
            available.add(t);
            reusable.add(t);
        }

        for (Theme t : Theme.all()) {
            if (available.contains(t) && reusable.contains(t)) {
                t.assignToFolder(outputFolder);
            }
        }
    }


    public static void main(String[] args){
        //System.setProperty("log4j.configurationFile","./src/main/resources/log4j2.xml");

        //Call the initializer so that Themes.name2theme are filled.
        new PatternHardExtractor();
        new WordnetExtractor();

        try {
            // Database setup
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException exception) {
            exception.printStackTrace();
            return;
        }


        MatchYago matchYago = new MatchYago();
        matchYago.startWorking();

    }
}
