import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Properties;

public class MicrosoftTranslatorAPI implements Translator {
    private static final Logger logger = LogManager.getLogger(MicrosoftTranslatorAPI.class);

    private String baseURL = "https://api.microsofttranslator.com/V2/Http.svc/";

    private String charset = "UTF-8";

    MicrosoftTranslatorAPI(){ }

    public String getResponse(String requestURL) throws IOException {
        //Get API key
        String api_key = TagstoYagoMatcher.getPROPERTIES().getProperty("MiscrosoftAPI.key");


        //Send HTTP Request
        URLConnection connection = new URL(requestURL).openConnection();
        connection.setRequestProperty("Ocp-Apim-Subscription-Key", api_key);
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

    public String detect(String textInOriginalLang) {
        String requestURL = null;

        try {
            requestURL = baseURL + "Detect?text=" + URLEncoder.encode(textInOriginalLang, charset);

            // Parse the response
            String strResponse = getResponse(requestURL);
            String sentinel = "<string xmlns=\"http://schemas.microsoft.com/2003/10/Serialization/\">";
            int startPosition = sentinel.length();
            int endPosition = strResponse.indexOf("</string>");
            String strResult = strResponse.substring(startPosition, endPosition);
            return strResult;


        } catch (IOException exception) {
            logger.error("Error: Microsoft Translator API failed on requesting: " + requestURL );
            exception.printStackTrace();
        }

        return "";
    }

    public String translate(String textInOriginalLang, String sourceLang, String targetLang) {
        String requestURL = null;


        String strResult;
        try {
            requestURL = baseURL + "Translate?from=" + sourceLang + "&to=" + targetLang + "&text=" + URLEncoder.encode(textInOriginalLang, charset);

            // Parse the response
            String strResponse = getResponse(requestURL);
            String sentinel = "<string xmlns=\"http://schemas.microsoft.com/2003/10/Serialization/\">";
            int startPosition = sentinel.length();
            int endPosition = strResponse.indexOf("</string>");
            strResult = strResponse.substring(startPosition, endPosition);
            logger.error("Finished: Paid Microsoft API finished requesting: " + requestURL );

        } catch (IOException exception) {
            logger.error("Error: Microsoft Translator API failed on requesting: " + requestURL );
            strResult = textInOriginalLang;
        }

        return strResult;
    }

    public static void main(String[] args){
        MicrosoftTranslatorAPI microsoftTranslatorAPI = new MicrosoftTranslatorAPI();

        String translationResult = microsoftTranslatorAPI.translate("我是中文",
                "", "en");

        String detectionResult = microsoftTranslatorAPI.detect("我是中文");

        System.out.println(translationResult);

    }


}
