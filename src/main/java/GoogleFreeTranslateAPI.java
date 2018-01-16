import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

public class GoogleFreeTranslateAPI {
    private static final Logger logger = LogManager.getLogger(GoogleFreeTranslateAPI.class);

    private String baseURL = "https://translate.googleapis.com/translate_a/single?client=gtx&sl=";
    private String charset = "UTF-8";
    private static final Object lockFailureCounter = new Object();

    public GoogleFreeTranslateAPI(){ }

    public String getResponse(String requestURL) throws IOException{

        //Send HTTP Request
        URLConnection connection = new URL(requestURL).openConnection();
        connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
        connection.setRequestProperty("Accept-Charset", charset);
        InputStream response = connection.getInputStream();

        // Get response into string
        BufferedReader streamReader = new BufferedReader(new InputStreamReader(response, charset));
        StringBuilder responseStrBuilder = new StringBuilder();
        String inputStr;
        while ((inputStr = streamReader.readLine()) != null) {
            responseStrBuilder.append(inputStr);
        }

        return responseStrBuilder.toString();
    }

    public String translate(String textInOriginalLang, String sourceLang, String targetLang) {
        String requestURL = null;

        try {
            requestURL = baseURL + sourceLang + "&tl=" + targetLang + "&dt=t&q=" + URLEncoder.encode(textInOriginalLang, charset);

            // Parse the response
            String strResponse = getResponse(requestURL);
            int startPosition = 4;
            int endPosition = strResponse.indexOf(textInOriginalLang);
            String strResult = strResponse.substring(startPosition, endPosition-3);
            return strResult;


        } catch (IOException exception) {
            logger.error("Error: free Google API failed on requesting: " + requestURL );
        }

        return "";
    }

    public String detect(String textInOriginalLang) {
        String requestURL = null;

        try {
            requestURL = baseURL + "auto&tl=en&dt=t&q=" + URLEncoder.encode(textInOriginalLang, charset);

            // Parse the response
            String strResponse = getResponse(requestURL);
            String strResult = strResponse.substring(strResponse.length()-6, strResponse.length()-4);
            return strResult;


        } catch (IOException exception) {
            logger.error("Error: free Google API failed on requesting: " + requestURL );
        }

        return "";
    }

    public static void main(String[] args){
        GoogleFreeTranslateAPI googleFreeTranslateAPI = new GoogleFreeTranslateAPI();

        String translationResult = googleFreeTranslateAPI.translate("Ансамль Спасо-Преображенского монастыря (Ярославская область, Ярославль, Богоявленская площадь, 25)",
                "ru", "en");

        String detectionResult = googleFreeTranslateAPI.detect("Ансамль Спасо-Преображенского монастыря (Ярославская область, Ярославль, Богоявленская площадь, 25)");

        System.out.println(translationResult);

    }


}
