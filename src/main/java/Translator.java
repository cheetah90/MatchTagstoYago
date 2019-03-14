import java.io.IOException;

public interface Translator {
    public String getResponse(String requestURL) throws IOException;
    public String detect(String textInOriginalLang);
    public String translate(String textInOriginalLang, String sourceLang, String targetLang);

}
