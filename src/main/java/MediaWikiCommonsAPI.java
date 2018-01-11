import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.*;


public class MediaWikiCommonsAPI {
    private static final Logger logger = LogManager.getLogger(MediaWikiCommonsAPI.class);

    private static int num_continuous_failures = 0;

    private static final Object lockFailureCounter = new Object();

    public class CommonsMetadata {
        public CommonsMetadata(String title, List<String> categories, String description, int pageID){
            this.title = title;
            this.categories = categories;
            this.description = description;
            this.pageID = pageID;
        }

        public CommonsMetadata() {}

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public List<String> getCategories() {
            return categories;
        }

        public void setCategories(List<String> categories) {
            this.categories = categories;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public int getPageID() {
            return pageID;
        }

        public void setPageID(int pageID) {
            this.pageID = pageID;
        }

        private String title;
        private List<String> categories;
        private String description;
        private int pageID;

    }


    private String baseURL = "https://commons.wikimedia.org/w/api.php?action=query&titles=";
    private String charset = "UTF-8";

    public MediaWikiCommonsAPI(){ }

    // Making this method thread-safe
    public List<CommonsMetadata> createMetadata(List<String> batch_filenames){
        // Initiate the return object
        List<CommonsMetadata> commonsMetadata_array = new ArrayList<>();

        // concat titles
        StringBuilder strBuilder = new StringBuilder();
        for (String title: batch_filenames) {
            strBuilder.append("|File:");
            strBuilder.append(title);
        }
        //remove the last "|"
        String titles_all = strBuilder.toString();
        titles_all = titles_all.substring(1);

        String requestURL = null;

        try{
            requestURL = baseURL + URLEncoder.encode(titles_all, charset) + "&prop=imageinfo&iiprop=extmetadata&format=json";

            //Send HTTP Request
            URLConnection connection = new URL(requestURL).openConnection();
            connection.setRequestProperty("Accept-Charset", charset);
            InputStream response = connection.getInputStream();

            // Get response into string
            BufferedReader streamReader = new BufferedReader(new InputStreamReader(response, charset));
            StringBuilder responseStrBuilder = new StringBuilder();
            String inputStr;
            while ((inputStr = streamReader.readLine()) != null) {
                responseStrBuilder.append(inputStr);
            }

            // Parse the json
            JSONObject resultsJSON = new JSONObject(responseStrBuilder.toString());
            JSONObject pagesJSON = resultsJSON.getJSONObject("query").getJSONObject("pages");


            // Parse each pages and add to return object
            Iterator<String> keys = pagesJSON.keys();

            while (keys.hasNext()) {
                // Initiate the COmmonsMetadata Objects
                CommonsMetadata metadata = new CommonsMetadata();

                try {
                    // set pageid
                    String pageid = keys.next();
                    metadata.setPageID(Integer.parseInt(pageid));

                    // Set title
                    String title = pagesJSON.getJSONObject(pageid).getString("title").substring(5).replaceAll(" ", "_");
                    metadata.setTitle(title);

                    //Parse and add categories
                    JSONObject extmetadata = pagesJSON.getJSONObject(pageid).getJSONArray("imageinfo").getJSONObject(0).getJSONObject("extmetadata");
                    String[] raw_Categories = extmetadata.getJSONObject("Categories").getString("value").split("\\|");
                    ArrayList<String> strCategories = new ArrayList<>();
                    for (String currentCat: raw_Categories) {
                        //preprocess the categories to conform to the standard in the matchingYago code
                        String strCat = currentCat.replaceAll(" ", "_");
                        strCategories.add(strCat);
                    }
                    metadata.setCategories(strCategories);

                    //Parse and add description
                    if (extmetadata.has("ImageDescription")){
                        String description = extmetadata.getJSONObject("ImageDescription").getString("value");
                        description = description.replaceAll("\\<.*?>","");     // Cheap way to remove html tags, more advanced option
                        metadata.setDescription(description);
                    } else {
                        metadata.setDescription("");    //at least set something
                    }
                } catch (JSONException exception) {
                    metadata = new CommonsMetadata();
                    logger.error("failed to parse one metadata!");
                }


                // Add to the return object
                commonsMetadata_array.add(metadata);
            }

        } catch (IOException exception) {
            logger.debug("Error accessing URL: " + requestURL);
            exception.printStackTrace();
            // Handle the error, if it's too hot, wait for a sec
            synchronized (lockFailureCounter) {
                if ( num_continuous_failures < 3){
                    num_continuous_failures++;
                } else {
                    num_continuous_failures = 0;
                    // The MediaWiki server might have detected us! Let's sleep for a while
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException interruptedEx) {
                        interruptedEx.printStackTrace();
                    }
                }
            }
        }


        return commonsMetadata_array;
    }

    public static void main(String[] args){
        MediaWikiCommonsAPI mediaWikiCommonsAPI = new MediaWikiCommonsAPI();

        List<String> file_titles = new ArrayList<>();
        file_titles.add("Groupe_Tribal_Percussions_-_250.jpg");
        file_titles.add("Trzebnica,_Poland_-_panoramio_(26).jpg");

        List<CommonsMetadata> commonsMetadataList = mediaWikiCommonsAPI.createMetadata(file_titles);

    }

}
