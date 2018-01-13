import basics.FactComponent;
import edu.stanford.nlp.simple.Sentence;
import javatools.parsers.Name;
import javatools.parsers.NounGroup;
import javatools.parsers.PlingStemmer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MatchCategory {
    private static final Logger logger = LogManager.getLogger(MatchCategory.class);

    /** Holds the nonconceptual categories */
    private Set<String> nonConceptualCategories = new HashSet<>();

    /** Holds the preferred meanings */
    private Map<String, String> preferredMeanings;

    private Connection yagoConnection;

    private HashMap<String, HashSet<String>> yagoLowercase2Original= null;

    private HashMap<String, HashSet<String>> yagoOriginal2Type = null;


    MatchCategory(Map<String, String> preferMeaning, Set<String> nonConceptualCategories, Connection yagoConnection){
        this.preferredMeanings = preferMeaning;
        this.nonConceptualCategories = nonConceptualCategories;
        this.yagoConnection = yagoConnection;
    }

    MatchCategory(Map<String, String> preferMeaning, Set<String> nonConceptualCategories, HashMap yagoLowercase2Original, HashMap yagoOriginal2Type) {
        this.preferredMeanings = preferMeaning;
        this.nonConceptualCategories = nonConceptualCategories;
        this.yagoLowercase2Original = yagoLowercase2Original;
        this.yagoOriginal2Type = yagoOriginal2Type;
    }

    public String directMatch(String text) {
        text = text.toLowerCase();

        // replace all spaces
        if (text.contains(" ")){
            text = text.replaceAll(" ", "_");
        }

        //First match to wordnet
        String match = this.preferredMeanings.get(text);
        if (match != null) return (match);

        //If not found, then match to YagoTyapes and YagoTaxonomy
        match = queryYagoDatabase(text);
        if (match != null) return (match);

        return null;

    }

    private ArrayList<String> filterNounsFromShortNounPhrase(String shortNounPhrase) {
        ArrayList<String> nouns = new ArrayList<>();

        Sentence sent = new Sentence(shortNounPhrase);

        List<String> postags = sent.posTags();

        List<String> tokens = sent.words();

        if (postags.size() != tokens.size()) {
            throw new Error("something is wrong");
        }

        for (int pos = 0 ; pos < tokens.size(); pos ++) {
            if (postags.get(pos).startsWith("NN")) {
                nouns.add(tokens.get(pos));
            }
        }

        return nouns;
    }

    private String returnHardcodedMapping(String category) {
        if (category.contains("coat_of_arms")) {
            return preferredMeanings.get("coat of arms");
        } else if (category.contains("football_kit")) {
            return directMatch("kit_(association_football)");
        } else if (category.contains("aerial_photographs")) {
            return directMatch("aerial_photography");
        } else if (category.contains("us_national_archives_location")){
            return directMatch("national_archives");
        } else if (category.contains("portable_antiquities_scheme")) {
            return directMatch("antiquities");
        } else if (category.contains("national_register_of_historic_places")) {
            return directMatch("national_register_of_historic_places");
        } else if (category.contains("oil_on_canvas") || category.contains("oil_on_panel")) {
            return directMatch("painting");
        } else if (category.contains("state_seals")) {
            return directMatch("united_states_state_seals");
        } else if (category.contains("locator_map")) {
            return directMatch("map");
        } else if (category.contains("demonstration")) {
            return directMatch("protest");
        } else if (category.startsWith("view_") || category.startsWith("views_")) {
            return directMatch("scenery");
        }

        return null;
    }

    //Extract the substring in the quote and match to Yago
    // The string in quotation is usually a hypernym (e.g. larger geography)
    private String matchQuotation(String categoryName) {
        String quote = null;
        String yagoMatch = null;

        if (categoryName.contains("'") ) {
            Pattern singleQuotePattern = Pattern.compile("\'([^\"]*)\'");
            Matcher m = singleQuotePattern.matcher(categoryName);
            while (m.find()) {
                quote = m.group(1);
            }

            Pattern doubleQuotePattern = Pattern.compile("\"([^\"]*)\"");
            m = doubleQuotePattern.matcher(categoryName);
            while (m.find()) {
                quote = m.group(1);
            }
        }

        if (quote != null) {
            yagoMatch = directMatch(quote);
        }

        return yagoMatch;
    }

    private String matchWithoutParenthesis(String category) {
        String yago_match = null;

        // remove the words in the parenthesis
        String current_categoryName = category.replaceAll("\\s*\\([^\\)]*\\)\\s*", " ");;
        // fix the tailing "_"
        current_categoryName = current_categoryName.replaceAll("_", " ").trim().replaceAll(" ", "_");
        // try direct match
        yago_match = directMatch(current_categoryName);

        return yago_match;
    }

    /**
     * First match the text without parenthesis and then the text inside the parenthesis
     * e.g. https://commons.wikimedia.org/wiki/File:Milevsko_okres_P%C3%ADsek_(3.).jpg
     * @param category
     * @return
     */
    private String matchInsideParenthesis(String category){
        String yago_match = null;

        // First match the string inside parenthesis
        Matcher m = Pattern.compile("\\s*\\([^\\)]*\\)\\s*").matcher(category);
        while (m.find()) {
            // Get rid of the parenthesis
            String content = m.group(0);
            content = content.substring(2, content.length()-1);
            yago_match = directMatch(content);
        }

        return yago_match;
    }

    // Wrong Acronym = an acronym (all uppercase letters) that does not exist in the original text
    private boolean isWrongAcronym(String yago_match, String text) {
        if (yago_match.length() < 3) {return true;}     // I am making arbitrary decision that anything shorter than two characters, though matched, it's probably bad acronym e.g. <Dp>

        yago_match = yago_match.substring(1,yago_match.length()-1);

        return (yago_match.equals(yago_match.toUpperCase()) && ! text.contains(yago_match));
    }

    boolean isValidYagoItem(String yagoitem, String original_text) {
        return (yagoitem != null && !yagoitem.contains("wordnet_image")
                && !yagoitem.contains("wordnet_location")
                && !yagoitem.contains("wordnet_picture")
                && !yagoitem.contains("wikicat_Years")
                && !yagoitem.contains("wordnet_part")
                && !yagoitem.contains("wordnet_detail")
                && !isWrongAcronym(yagoitem, original_text));
    }

    private String parseAndMatch(String original_categoryName){
        String yagoitem;

        //If not a existing yago entity, parse category to match a yagoitem
        original_categoryName = FactComponent.stripCat(original_categoryName);

        // Check out whether the new category is worth being added
        // If the word is too short to be a head
        NounGroup original_category = new NounGroup(original_categoryName);
        NounGroup lower_category = new NounGroup(original_categoryName);
        if (original_category.head() == null || original_category.head().length() < 3) {
            return (null);
        }

        // If the category is an acronym, drop it
        if (Name.isAbbreviation(original_category.head())) {
            //System.out.println("Could not find type in" + categoryName +"(is abbreviation)");
            return (null);
        }

        // extract the stemmed head
        String lower_stemmedHead = PlingStemmer.stem(lower_category.head());
        boolean isStemmedHeadDifferent = ! lower_stemmedHead.equals(lower_category.head());

        // Exclude the bad guys
        if (nonConceptualCategories.contains(lower_stemmedHead) || nonConceptualCategories.contains(lower_category.head())) {
            //System.out.println("Could not find type in" + categoryName + "(is non-conceptual)");
            return (null);
        }

        // Try all premodifiers (reducing the length in each step) + head
        if (lower_category.preModifier() != null) {
            String preModifier = lower_category.preModifier().replace('_', ' ');

            for (int start = 0; start != -1 && start < preModifier.length() - 2; start = preModifier.indexOf(' ', start + 1)) {
                String current_premodifier = (start == 0 ? preModifier : preModifier.substring(start + 1));
                String current_noungroup = current_premodifier + " " + lower_stemmedHead;
                current_noungroup = current_noungroup.replaceAll(" ", "_");
                yagoitem = directMatch(current_noungroup);

                // take the longest matching sequence
                if (isValidYagoItem(yagoitem, original_categoryName)) return (yagoitem);

                //try using the original head
                if (isStemmedHeadDifferent) {
                    current_noungroup = current_premodifier + " " + lower_category.head();
                    current_noungroup = current_noungroup.replaceAll(" ", "_");
                    yagoitem = directMatch(current_noungroup);

                    if (isValidYagoItem(yagoitem, original_categoryName)) return (yagoitem);
                }

            }
        }

        // Try postmodifiers to catch "head of state"
        if (lower_category.postModifier() != null && lower_category.preposition() != null && lower_category.preposition().equals("of")) {
            String searchString = lower_stemmedHead + " of " + lower_category.postModifier().head();
            searchString = searchString.replaceAll(" ", "_");

            yagoitem = directMatch(searchString);
            if (isValidYagoItem(yagoitem, original_categoryName)) return (yagoitem);

            // try using the original head
            if (isStemmedHeadDifferent) {
                searchString = lower_category.head() + " of " + lower_category.postModifier().head();
                searchString = searchString.replaceAll(" ", "_");

                yagoitem = directMatch(searchString);
                if (isValidYagoItem(yagoitem, original_categoryName)) return (yagoitem);
            }

        }

        // Try stemmed head
        yagoitem = directMatch(lower_stemmedHead);
        if (isValidYagoItem(yagoitem, original_categoryName) && containsWithoutCapitalization(yagoitem, original_categoryName)) return (yagoitem);

        // Try head
        if (isStemmedHeadDifferent) {
            yagoitem = directMatch(lower_category.head());
            if (isValidYagoItem(yagoitem, original_categoryName) && containsWithoutCapitalization(yagoitem, original_categoryName)) return (yagoitem);
        }

        // deal with the parenthesis case
        if (original_categoryName.contains("(")) {
            yagoitem = matchInsideParenthesis(original_categoryName);
            if (isValidYagoItem(yagoitem, original_categoryName)) {return yagoitem;}
        }

        // If so far nothing is matched and there are only two words, try to match the non-trivial premodifier
        if (lower_category.toString().split(" ").length == 2 &&
                (lower_category.preModifier() != null) &&
                lower_category.preModifier().length() > 5) {
            yagoitem = directMatch(lower_category.preModifier());
            if (isValidYagoItem(yagoitem, original_categoryName) && containsWithoutCapitalization(yagoitem, original_categoryName)) {return yagoitem;}
        }

        return (null);
    }

    private boolean containsWithoutCapitalization(String yagoitem, String original_text) {
        if (yagoitem.startsWith("<wordnet")) {
            return true;
        }

        if (yagoitem.startsWith("<wikicat_")) {
            yagoitem = yagoitem.replaceAll("wikicat_", "");
        }

        yagoitem = yagoitem.substring(1,yagoitem.length()-1);

        String caped_yagoitem = yagoitem.substring(0,1).toUpperCase() + yagoitem.substring(1);
        String uncaped_yagoitem = yagoitem.substring(0,1).toLowerCase() + yagoitem.substring(1);

        return (original_text.contains(caped_yagoitem) || original_text.contains(uncaped_yagoitem) || original_text.contains(yagoitem));
    }

    /**
     * This method is adapted from yago's project https://github.com/yago-naga/yago3/blob/ab78ee41a97c62307bb148ba862b09dbd7e67d08/src/main/java/fromThemes/CategoryClassExtractor.java
     */
    public String matchNounGroup2Yago(String original_categoryName){
        String yagoitem;

        // preprocess the original categoryName
        if (original_categoryName.contains(" ")) {
            original_categoryName = original_categoryName.replaceAll(" ", "_");
        }
        original_categoryName = original_categoryName.toLowerCase();

        //Hardcoded categories
        yagoitem = returnHardcodedMapping(original_categoryName);
        if (isValidYagoItem(yagoitem, original_categoryName)) {return yagoitem;}

        // Check if the category itself is a yago entity or class
        yagoitem = directMatch(original_categoryName);
        if (isValidYagoItem(yagoitem, original_categoryName)) {return yagoitem;}

        // Check if it contains quotations. If yes, match the substring in the quotation
        yagoitem = matchQuotation(original_categoryName);
        if (isValidYagoItem(yagoitem, original_categoryName)) {return yagoitem;}

        // If it contains parenthesis, first match things outside parenthesis
        if (original_categoryName.contains("(")){
            // Check if it contains parenthesis. If yes, match the string without parenthesis directly first
            yagoitem = matchWithoutParenthesis(original_categoryName);
            if (isValidYagoItem(yagoitem, original_categoryName)) {return yagoitem;}
        }

        // If it contains comma, match the first phrase
        if (original_categoryName.contains(",")){
            yagoitem = directMatch(original_categoryName.split(",")[0].replaceAll("_"," ").trim().replaceAll(" ", "_"));
            if (isValidYagoItem(yagoitem, original_categoryName)) {return yagoitem;}
        }

        // Parse the noun group to match
        yagoitem = parseAndMatch(original_categoryName);
        if (yagoitem != null) {return yagoitem;}      // We checked for validity inside func:parseAndMatch

        return null;

    }

    String title2class(String title) {
        title = title.toLowerCase().replaceAll("-", "_");

        //Check if the category itself is a yago entity or class
        String yagoitem = directMatch(title);
        if (isValidYagoItem(yagoitem, title)) {return yagoitem;}

        ArrayList<String> nouns = filterNounsFromShortNounPhrase(title.replaceAll("_", " "));

        // if too many nouns, we are confused so we quit
        if (nouns.size() < 3) {
            for (String noun: nouns) {
                yagoitem = directMatch(noun);
                if (isValidYagoItem(yagoitem, title) && containsWithoutCapitalization(yagoitem, title)) return yagoitem;
            }
        }

        return null;

    }

    boolean hadValidObject(String typeInfo) {
        return (!typeInfo.contains("wikicat_Abbreviations") && !typeInfo.contains("wordnet_first_name"));
    }

    private String querySQLDatabase(String queryString) {
        try {
            PreparedStatement stmt = null;

            // if it corresponds to a entity name, return its type
            String query_yagotype = "SELECT * FROM YAGOTYPES WHERE LOWER(subject) = ? ";
            stmt = this.yagoConnection.prepareStatement(query_yagotype);
            stmt.setString(1, "<" + queryString + ">");
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String typeInfo = rs.getString("object");
                String subjectInfo = rs.getString("subject");
                if (hadValidObject(typeInfo) ){
                    return subjectInfo;
                }

            }
            rs.close();
            stmt.close();

            // if it corresponds to a category
            String query_yagotaxonomy = "SELECT * FROM YAGOTAXONOMY WHERE LOWER(subject) = ? ";
            stmt = this.yagoConnection.prepareStatement(query_yagotaxonomy);
            stmt.setString(1, "<wikicat_" + queryString + ">");
            rs = stmt.executeQuery();
            while ( rs.next() ) {
                String typeInfo = rs.getString("object");
                String subjectInfo = rs.getString("subject");
                if (hadValidObject(typeInfo)){
                    return subjectInfo;
                }
            }

            rs.close();
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    private String queryinMemory(String queryString) {
        String queryStringAsEntity = "<" + queryString + ">";
        String queryStringAsCategory = "<wikicat_" + queryString + ">";


        // if the NonCat exists
        HashSet<String> resultset = this.yagoLowercase2Original.get(queryStringAsEntity);
        if ( resultset != null){
            if (resultset.size() >1) {
                //logger.info("More than one originals for this lowercase, so I output all!");
                for (String yagoitem: resultset) {
                    return yagoitem;
                }
            }
            else {
                for (String yagoitem: resultset) {
                    return yagoitem;
                }
            }
        }

        // if Cat exis
        resultset = this.yagoLowercase2Original.get(queryStringAsCategory);
        if ( resultset != null){
            if (resultset.size() >1) {
                //logger.info("More than one originals for this lowercase, so I output all!");
                for (String yagoitem: resultset) {
                    return yagoitem;
                }
            }
            else {
                for (String yagoitem: resultset) {
                    return yagoitem;
                }
            }
        }



        return null;
    }

    private String queryYagoDatabase(String queryString){
        // Query SQL or Query inMemory
        if (this.yagoConnection != null) {
            return querySQLDatabase(queryString);
        } else {
            return queryinMemory(queryString);
        }
    }

}
