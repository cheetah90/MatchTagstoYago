import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import org.json.*;


public class MediaWikiCommonsAPI {
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


    private String baseURL = "https://commons.wikimedia.org/w/api.php?action=query&titles=File:";
    private String charset = "UTF-8";

    public MediaWikiCommonsAPI(){ }

    // Making this method thread-safe
    public CommonsMetadata createMeatadata(String fileName){
        CommonsMetadata metadata = new CommonsMetadata();
        metadata.setTitle(fileName);
        String requestURL = null;

        try {
            requestURL = baseURL + URLEncoder.encode(fileName, charset) + "&prop=imageinfo&iiprop=extmetadata&format=json";
            //System.out.println("(in try block) Accessing URL: " + requestURL);

            //Send HTTP Request
            URLConnection connection = new URL(requestURL).openConnection();
            connection.setRequestProperty("Accept-Charset", charset);
            InputStream response = connection.getInputStream();

            BufferedReader streamReader = new BufferedReader(new InputStreamReader(response, charset));
            StringBuilder responseStrBuilder = new StringBuilder();

            String inputStr;
            while ((inputStr = streamReader.readLine()) != null)
                responseStrBuilder.append(inputStr);
            JSONObject resultsJSON = new JSONObject(responseStrBuilder.toString());
            JSONObject page = resultsJSON.getJSONObject("query").getJSONObject("pages");
            if (page.keys().hasNext()) {
                String pageid = page.keys().next();
                JSONObject extmetadata = page.getJSONObject(pageid).getJSONArray("imageinfo").getJSONObject(0).getJSONObject("extmetadata");

                // set pageid
                metadata.setPageID(Integer.parseInt(pageid));

                //Parse and add categories
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

            }

        } catch (IOException exception) {
            //System.out.println("(in catch block) Accessing URL: " + requestURL);
            exception.printStackTrace();
        }


        return metadata;
    }

    public static void main(String[] args){
        MediaWikiCommonsAPI mediaWikiCommonsAPI = new MediaWikiCommonsAPI();

        CommonsMetadata commonsMetadata = mediaWikiCommonsAPI.createMeatadata("Plenário_do_Senado_(18252811193).jpg");

    }

}
