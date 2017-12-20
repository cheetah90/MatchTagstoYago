import javatools.administrative.Parameters;
import fromOtherSources.PatternHardExtractor;
import fromOtherSources.WordnetExtractor;
import utils.Theme;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.cloud.translate.*;

public class MatchYago {

    public static Properties getPROPERTIES() {
        return PROPERTIES;
    }

    private static final Properties PROPERTIES = new Properties();

    private static final Logger logger = LogManager.getLogger(MatchYago.class);

    private Connection db4SamplesConnection;

    private Connection yagoConnection;

    private Translate googleTranslate;

    private final static HashMap<String, HashSet<String>> yagoLowercase2Original = new HashMap<>();

    private final static HashMap<String, HashSet<String>> yagoOriginal2Type = new HashMap<>();

    /** Holds the preferred meanings */
    protected Map<String, String> preferredMeanings;

    // Holds the nonconceptual categorie
    protected Set<String> nonConceptualCategories = new HashSet<>();

    MatchCategory matchCategory;

    public MatchYago(){

        try {
            // necessary load for yago
            nativeYagoLoad();

            //Load properties file

            PROPERTIES.load(new InputStreamReader(new FileInputStream("./src/main/resources/config.properties"), "UTF8"));

            // Load the samples
            this.db4SamplesConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Samples.port")+"/"+PROPERTIES.getProperty("db4Samples.name"),
                    PROPERTIES.getProperty("db4Samples.username"), PROPERTIES.getProperty("db4Samples.password"));


            // Initialize hardcoded mappings and set the static variable
            ProcessSingleImageRunnable.setPreferredMeanings(WordnetExtractor.PREFMEANINGS.factCollection().getPreferredMeanings());
            ProcessSingleImageRunnable.setNonConceptualCategories(PatternHardExtractor.CATEGORYPATTERNS.factCollection().seekStringsOfType("<_yagoNonConceptualWord>"));


        } catch (IOException | SQLException  e) {
            e.printStackTrace();
        }

        logger.info("Start to load Yago data...");

        // Initialize connection to Yago's sample
        if (PROPERTIES.getProperty("LoadYago2Memory").equals("true")) {
            logger.info("Choose to load Yago data into memory!");
            loadYagotoMemory();
            ProcessSingleImageRunnable.setYagoLowercase2Original(yagoLowercase2Original);
            ProcessSingleImageRunnable.setYagoOriginal2Type(yagoOriginal2Type);

        } else {
            logger.info("Choose to load Yago data from a database!");
            try {
                yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4Yago.name"),
                        PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

                ProcessSingleImageRunnable.setYagoConnection(yagoConnection);

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        logger.info("Finish loading Yago data...");

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
                !typeInfo.contains("wikicat_Abbreviations") && !typeInfo.contains("wordnet_first_name"));
    }

    private void loadResultSet(ResultSet rs) throws SQLException {
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
//            else {
//                logger.info("Invalid yago record: subject=" + subject + " / object=" + object);
//            }

        }
    }

    private void loadYagotoMemory(){
        try {
            Connection yagoConnection = DriverManager.getConnection("jdbc:postgresql://localhost:"+PROPERTIES.getProperty("db4Yago.port")+"/"+PROPERTIES.getProperty("db4Yago.name"),
                    PROPERTIES.getProperty("db4Yago.username"), PROPERTIES.getProperty("db4Yago.password"));

            PreparedStatement stmt = null;
            String yagotypesTable = PROPERTIES.getProperty("debugMode").equals("true") ? "subset_yagotypes" : "yagotypes";

            // load yagotypes
            String query_yagotype = "SELECT * FROM "+ yagotypesTable;
            stmt = yagoConnection.prepareStatement(query_yagotype);
            ResultSet rs = stmt.executeQuery();
            //Load the resultset
            loadResultSet(rs);
            rs.close();
            stmt.close();


            if (!PROPERTIES.getProperty("debugMode").equals("true")) {
                // Load the yagotaxonomy
                String query_yagotaxonomy = "SELECT * FROM YAGOTAXONOMY";
                stmt = yagoConnection.prepareStatement(query_yagotaxonomy);
                rs = stmt.executeQuery();
                loadResultSet(rs);
                rs.close();
                stmt.close();
            }


        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }


    private void startWorking(){
        //clearOutputfile
        clearOutputfile("./output_per_tag.tsv");
        clearOutputfile("./output_per_img.tsv");

        List<String> pageTitles = null;
        int numofImages;

        try {
            pageTitles = queryAllPageTitles();
            numofImages = pageTitles.size();
            logger.info("Total number of images to process: " + numofImages);
        } catch (SQLException exception) {
            exception.printStackTrace();
            logger.error("Failed to load the name of the images to be processed.");
            return;
        }

        for (String original_title: pageTitles){
            (new Thread(new ProcessSingleImageRunnable(original_title))).start();
        }

        while (ProcessSingleImageRunnable.getCounter() < numofImages) {
            System.out.println("Processing " + ProcessSingleImageRunnable.getCounter() + "/"+numofImages);
            try {
                synchronized (this) {
                    this.wait(10000);
                }
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            }
        }
        System.out.println("Processing " + ProcessSingleImageRunnable.getCounter() + "/"+numofImages);
        logger.info("# of Flickr images: " + ProcessSingleImageRunnable.getFlickrCounter());
        logger.info("# of Panoramio images:" + ProcessSingleImageRunnable.getPanoramioCounter());

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
