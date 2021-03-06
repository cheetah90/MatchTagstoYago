import javatools.administrative.Parameters;
import fromOtherSources.PatternHardExtractor;
import fromOtherSources.WordnetExtractor;
import utils.Theme;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TagstoYagoMatcher {

    public static Properties getPROPERTIES() {
        return PROPERTIES;
    }

    private static final Properties PROPERTIES = new Properties();

    private static final Logger logger = LogManager.getLogger(TagstoYagoMatcher.class);

    private static final String TRANSLATIONCACHEFILENAME = "./data/translationCache.ser";

    private static final String PARENTCATSCACHEFILENAME = "./data/parentCats.tsv";

    private final static HashMap<String, HashSet<String>> yagoLowercase2Original = new HashMap<>();

    private final static HashMap<String, HashSet<String>> yagoOriginal2Type = new HashMap<>();

    /** Holds the preferred meanings */
    protected Map<String, String> preferredMeanings;

    private void deserializeCaches(){
        // deserialize or initialize the cache for category - parent category mapping
        File f = new File(PARENTCATSCACHEFILENAME);
        if (f.exists() && !f.isDirectory()) {
            try {
                BufferedReader br = new BufferedReader((new FileReader(PARENTCATSCACHEFILENAME)));
                ConcurrentHashMap<String, List<String>> catToParentCats= new ConcurrentHashMap<>();
                String line;
                while ((line = br.readLine()) != null) {
                    String[] content = line.split("\t");
                    if (content.length == 2 && catToParentCats.get(content[0]) == null) {
                        catToParentCats.put(content[0], Arrays.asList(content[1].split("<>")));
                    }
                }
                ProcessBatchImageRunnable.setcachedParentCategories(catToParentCats);
            } catch (IOException i) {
                i.printStackTrace();
            }
        } else {
            ProcessBatchImageRunnable.setcachedParentCategories(new ConcurrentHashMap<>());
        }


        f = new File(TRANSLATIONCACHEFILENAME);
        if (f.exists() && !f.isDirectory()) {
            try {
                FileInputStream fileIn = new FileInputStream(TRANSLATIONCACHEFILENAME);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                ProcessBatchImageRunnable.setCachedTranslation((ConcurrentHashMap<String, String>) in.readObject());
                in.close();
                fileIn.close();

            } catch (IOException i) {
                i.printStackTrace();
            } catch (ClassNotFoundException c) {
                logger.error("Can't read the serializable into cacheTranslation");
            }
        } else {
            ProcessBatchImageRunnable.setCachedTranslation(new ConcurrentHashMap<>());
        }

    }

    public TagstoYagoMatcher(){

        try {
            // necessary load for yago
            nativeYagoLoad();

            //Load properties file
            PROPERTIES.load(new InputStreamReader(new FileInputStream("./src/main/resources/config.properties"), "UTF8"));

            // Initialize hardcoded mappings and set the static variable
            ProcessBatchImageRunnable.setPreferredMeanings(WordnetExtractor.PREFMEANINGS.factCollection().getPreferredMeanings());
            ProcessBatchImageRunnable.setNonConceptualCategories(PatternHardExtractor.CATEGORYPATTERNS.factCollection().seekStringsOfType("<_yagoNonConceptualWord>"));


        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("Start to load Yago data...");

        // Initialize connection to Yago's sample
        if (PROPERTIES.getProperty("LoadYago2Memory").equals("true")) {
            logger.info("Choose to load Yago data into memory!");
            loadYagotoMemory();
            ProcessBatchImageRunnable.setYagoLowercase2Original(yagoLowercase2Original);
            ProcessBatchImageRunnable.setYagoOriginal2Type(yagoOriginal2Type);

        } else {
            logger.info("Choose to load Yago data from a database!");
            try {
                Connection yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4Yago.name"),
                        PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

                ProcessBatchImageRunnable.setYagoConnection(yagoConnection);

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        logger.info("Finish loading Yago data...");

        // load the linux words
        HashSet<String> linuxWords = new HashSet<>();
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("data/linux.words"));
            String line;
            while ((line = bufferedReader.readLine()) != null)
            {
                linuxWords.add(line);
            }

            ProcessBatchImageRunnable.setlinuxEnglishWord(linuxWords);
        } catch (IOException e) {
        }

        // deserialize the object
        deserializeCaches();

    }

    private void clearOutputfile(String outputFileName){
        try {
            Writer output = new BufferedWriter(new FileWriter(outputFileName, false));
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private boolean isValidObject(String typeInfo) {
        return (typeInfo != null &&
                !typeInfo.contains("wikicat_Abbreviations")
                && !typeInfo.contains("wordnet_first_name")
                && !typeInfo.contains("wordnet_surname"));
    }

    private void loadForeignWikiResultSet(ResultSet rs) throws SQLException {
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
                // If this is a multilingual word
                if (subject.length() > 5 && subject.substring(1,4).matches("[a-zA-Z]{2}/")) {
                    String strip_subject = "<"+subject.substring(4);

                    // first add to yagoLowercase2Original
                    // if this foreign entity does not exist in en.wiki, add it without the lang code
                    if (yagoLowercase2Original.get(strip_subject.toLowerCase()) == null) {
                        // the lowercase does not exist
                        HashSet<String> hashSet = new HashSet<>();
                        hashSet.add(strip_subject);
                        yagoLowercase2Original.put(strip_subject.toLowerCase(), hashSet);
                    } else {
                        yagoLowercase2Original.get(strip_subject.toLowerCase()).add(strip_subject);
                    }

                    // Then add to yagoOriginal2Type
                    if (yagoOriginal2Type.get(strip_subject) == null) {
                        // the lowercase does not exist
                        HashSet<String> hashSet = new HashSet<>();
                        hashSet.add(object);
                        yagoOriginal2Type.put(strip_subject, hashSet);
                    } else {
                        yagoOriginal2Type.get(strip_subject).add(object);
                    }
                }
            } else {
                //logger.debug("Invalid yago record: subject=" + subject + " / object=" + object);
            }
        }
    }

    private void loadEnWikiResultSet(ResultSet rs) throws SQLException {
        while (rs.next()) {
            String subject = rs.getString("subject");
            String object = rs.getString("object");
            String predicate = rs.getString("predicate");

            if (isValidObject(object) && subject != null && !(predicate.equals("rdf:redirect") && subject.toLowerCase().equals(object.toLowerCase()))){
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
            else {
                //logger.debug("Invalid yago record: subject=" + subject + " / object=" + object);
            }

        }
    }

    private void loadYagotoMemory(){
        try {
            Connection yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4Yago.name"),
                    PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

            PreparedStatement stmt = null;
            String yagotypesTable = PROPERTIES.getProperty("debugLocally").equals("true") ? "subset_yagotypes" : "yagotypes";

            // local debug mode: only load subset of types
            if (PROPERTIES.getProperty("debugLocally").equals("true")) {
                String query_yagotype = "SELECT * FROM subset_yagotypes";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                ResultSet rs = stmt.executeQuery();
                //Load the resultset
                loadEnWikiResultSet(rs);
                rs.close();
                stmt.close();
            } else {
                // load all dataset
                // 1) load the enwiki yagotypes
                String query_yagotype = "SELECT * FROM yagotypes_enwiki";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                ResultSet rs = stmt.executeQuery();
                //Load the resultset
                loadEnWikiResultSet(rs);
                rs.close();
                stmt.close();

                // 2) load the foreign yagotypes
                query_yagotype = "SELECT * FROM yagotypes_foreignwiki";
                stmt = yagoConnection.prepareStatement(query_yagotype);
                rs = stmt.executeQuery();
                //Load the resultset
                loadForeignWikiResultSet(rs);
                rs.close();
                stmt.close();

                // 3 Load the yagotaxonomy
                String query_yagotaxonomy = "SELECT * FROM YAGOTAXONOMY";
                stmt = yagoConnection.prepareStatement(query_yagotaxonomy);
                rs = stmt.executeQuery();
                loadEnWikiResultSet(rs);
                rs.close();
                stmt.close();
            }


        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    private int getLineNumberofFile(String file_ImageNames) {
        try {
            Process p = Runtime.getRuntime().exec("wc -l " + file_ImageNames);
            p.waitFor();

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = reader.readLine();
            return Integer.parseInt(line.trim().split(" ")[0]);

        } catch (Exception exception) {
            exception.printStackTrace();
        }

        return 0;

    }

    private void serializeCacheObjects(Object cacheObject, String filename){
        // save the translationCache to the file since translation cost money
        try {
            FileOutputStream fileOut = new FileOutputStream(filename);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(cacheObject);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            logger.error("Serialization of " + filename + "failed!");
        }
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

    private void writeCachedParentCategories() {
        // replace the parentCat.tsv files
        File file_old = new File("data/parentCats.tsv");
        if (file_old.exists()) {
            file_old.delete();
        }


        for (String key: ProcessBatchImageRunnable.getCachedParentCategories().keySet()) {
            String lineParentCats = "";
            List<String> cachedParentCategories = ProcessBatchImageRunnable.getCachedParentCategories().get(key);

            if (cachedParentCategories.size() > 0) {
                lineParentCats = String.join("<>", cachedParentCategories);
            }
            String lineToWrite = key + "\t" + lineParentCats;
            appendLinetoFile(lineToWrite, "data/parentCats.tsv");
        }
    }

    private void startWorking(){
        //clearOutputfile
        clearOutputfile("./output_per_tag.tsv");
        clearOutputfile("./output_per_img.tsv");
        clearOutputfile("./output_per_img_parcat.tsv");
//        clearOutputfile("./output_cat2yago.tsv");
//        clearOutputfile("./aux_cat_needs_parents.tsv");

        String file_ImageNames = "./image_names.txt";

        int numofImages = getLineNumberofFile(file_ImageNames);
        logger.info("Total number of images to process: " + numofImages);

        try {
            // Buffered read the file
            BufferedReader br = new BufferedReader(new FileReader(file_ImageNames));
            String a_line;
            int line_counter = 0;

            ExecutorService pool = null;
            ArrayList<String> batch_imageCats = null;


            // Read through the file
            while ((a_line = br.readLine()) != null) {
                // Split by 100k lines
                if (line_counter % Integer.parseInt(PROPERTIES.getProperty("numImgsInSplit"))== 0) {
                    // if reachings 1m, shutdown the pool, wait until all tasks have completed
                    if (pool != null) {
                        // report the current status after this split.
                        System.out.println("Finished processing " + ProcessBatchImageRunnable.getCompletedCounter() + "/"+numofImages);
                        logger.info("Finished processing " + ProcessBatchImageRunnable.getCompletedCounter() + "/"+numofImages);

                        //Finished creating the threads
                        pool.shutdown();
                        // Wait until these tasks finished
                        if (!pool.awaitTermination(100, TimeUnit.MINUTES)) {
                            System.out.println("Error: reached the ExecutorService timeout!");
                            logger.error("Error: reached the ExecutorService timeout!");
                            pool.shutdownNow();
                        }
                    }

                    pool = Executors.newFixedThreadPool(Integer.parseInt(PROPERTIES.getProperty("maxThreadPool")));
                }

                // For every 100, assign to a thread
                if (line_counter % Integer.parseInt(PROPERTIES.getProperty("numImgsInBatch"))== 0) {
                    if (batch_imageCats != null && batch_imageCats.size() > 0) {
                        pool.execute(new ProcessBatchImageRunnable(new ArrayList<>(batch_imageCats)));
                    }
                    //Create a new batch_imageCats for next batch
                    batch_imageCats = new ArrayList<>();
                }

                batch_imageCats.add(a_line);

                line_counter++;
            }

            // process the remaining batch
            pool.execute(new ProcessBatchImageRunnable(new ArrayList<>(batch_imageCats)));
            //Finished creating the threads
            pool.shutdown();
            // Wait until these tasks finished
            if (!pool.awaitTermination(100, TimeUnit.MINUTES)) {
                System.out.println("Error: reached the ExecutorService timeout!");
                logger.error("Error: reached the ExecutorService timeout!");
                pool.shutdownNow();
            }


        } catch (Exception exception) {
            logger.error("filenames.txt does not exist!");
            exception.printStackTrace();
        }

        // serialize cache object
        serializeCacheObjects(ProcessBatchImageRunnable.getCachedTranslation(), "./data/translationCache.ser");
        writeCachedParentCategories();


        System.out.println("Finished processing " + ProcessBatchImageRunnable.getCompletedCounter() + "/"+numofImages);
        logger.info("# of Flickr images: " + ProcessBatchImageRunnable.getFlickrCounter());
        logger.info("# of Panoramio images:" + ProcessBatchImageRunnable.getPanoramioCounter());
        logger.info("# of images failured on MediaWikiAPI: " + ProcessBatchImageRunnable.getFailedImageCounter());
        logger.info("# of valid images: " + ProcessBatchImageRunnable.getValidPhotoCounter());
        logger.info("# of characters to translate: " + ProcessBatchImageRunnable.ChartoTranslateCounter);

        // profile the execution time
        logger.info("The avg execution time for MediaWikipediaAPI() is: " + ProcessBatchImageRunnable.time_mediaWikipeida.getMean());
        logger.info("The avg execution time for preprocessCommonsMetadata() is: " + ProcessBatchImageRunnable.time_preprocessCommonsMetadata.getMean());
        logger.info("The avg execution time to process one category is: " + ProcessBatchImageRunnable.time_processOneCategory.getMean());
        logger.info("The avg execution time to process one description is: " + ProcessBatchImageRunnable.time_processOneDescription.getMean());
        logger.info("The avg execution time to process one batch of images (" + PROPERTIES.getProperty("numImgsInBatch") + ") is: " + ProcessBatchImageRunnable.time_oneBatch.getMean());
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

        TagstoYagoMatcher tagstoYagoMatcher = new TagstoYagoMatcher();
        tagstoYagoMatcher.startWorking();

    }
}
