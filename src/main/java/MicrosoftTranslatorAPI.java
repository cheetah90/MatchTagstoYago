import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MicrosoftTranslatorAPI implements Translator {
    private static final Logger logger = LogManager.getLogger(MicrosoftTranslatorAPI.class);

    private String baseURL = "https://api.cognitive.microsofttranslator.com/";

    private String charset = "UTF-8";

    public void setApi_key(String api_key) {
        this.api_key = api_key;
    }

    private String api_key = TagstoYagoMatcher.getPROPERTIES().getProperty("MiscrosoftAPI.key");

    MicrosoftTranslatorAPI(){ }

    public String getResponse(String requestURL, String originalText) throws IOException {
        HttpClient httpclient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(requestURL);

        httpPost.setHeader("Ocp-Apim-Subscription-Key", api_key);
        httpPost.setHeader("Content-type", "application/json");

        String jsonString = new JSONArray().put(new JSONObject().put("Text", originalText)).toString();
        StringEntity requestEntity = new StringEntity(jsonString, ContentType.APPLICATION_JSON);
        httpPost.setEntity(requestEntity);

        HttpResponse rawResponse = httpclient.execute(httpPost);
        HttpEntity entity = rawResponse.getEntity();

        if (entity != null) {
            try (InputStream instream = entity.getContent()) {
                return IOUtils.toString(instream, StandardCharsets.UTF_8);
            }
        }

        return "";
    }

    public String detect(String textInOriginalLang) {
        String requestURL = null;

        try {
            requestURL = baseURL + "Detect?text=" + URLEncoder.encode(textInOriginalLang, charset);

            // Parse the response
            String strResponse = getResponse(requestURL, textInOriginalLang);
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
            requestURL = baseURL + "translate?api-version=3.0&to=" + targetLang;

            // Parse the response
            String strResponse = getResponse(requestURL, textInOriginalLang);
            JSONObject obj = new JSONArray(strResponse).getJSONObject(0);
            strResult = obj.getJSONArray("translations").getJSONObject(0).getString("text");

        } catch (IOException exception) {
            logger.error("Error: Microsoft Translator API failed on requesting: " + requestURL );
            strResult = textInOriginalLang;
        }

        return strResult;
    }

    public static void main(String[] args){
        MicrosoftTranslatorAPI microsoftTranslatorAPI = new MicrosoftTranslatorAPI();

        microsoftTranslatorAPI.setApi_key("37974102264b4397af4cd0703373e956");

        String translationResult = microsoftTranslatorAPI.translate("我是中国人",
                "", "en");

        String detectionResult = microsoftTranslatorAPI.detect("我是中国人");

        System.out.println(translationResult);

    }


}
