package restapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import oauth.signpost.OAuthConsumer;
import oauth.signpost.basic.DefaultOAuthConsumer;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import utils.OAuthUtils;


/**
 *
 * @author ram
 */
public class QuerySearch {
    
    public OAuthConsumer getConsumer(){
        OAuthConsumer consumer =
                new DefaultOAuthConsumer(OAuthUtils.CONSUMER_KEY, OAuthUtils.CONSUMER_SECRET);
        consumer.setTokenWithSecret(OAuthUtils.ACCESS_TOKEN, OAuthUtils.ACCESS_TOKEN_SECRET);
        return consumer;
    }
    
    
   
    public JSONArray getTweets(String query){
        JSONArray tweets = new JSONArray();
        BufferedReader breader = null;
        // below is the maximum no of tweets per page according to twiitter api..
        int tweets_per_page = 100;
        int count = -1;
        String  encoded_query = query;// incase encoding fails
        try {
            encoded_query = URLEncoder.encode(query, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            System.out.println(ex.getMessage());
        }
        String base_url = "https://api.twitter.com/1.1/search/tweets.json?"
                    + "q="+encoded_query
                    +"&count="+tweets_per_page
                    +"&result_type=mixed";
        long max_id = 0;
        long since_id = 0;
        //Here we are getting only 500 tweets....just to show you that the search works
        try { 
            while(count != 0 && tweets.length()<500){                
                URL url = null;
                if(max_id == 0) //it is the first request...
                     url = new URI(base_url).toURL();
                else
                    url = new URI(base_url+"&max_id="+(max_id-1)
                            +"&since_id="+since_id).toURL();
                System.out.print("getting tweets for: ");
                System.out.println(url.toString());      
                HttpURLConnection huc = (HttpURLConnection) url.openConnection();
                huc.setReadTimeout(5000); // for 5 seconds..
                //get authenticated with twitter..
                OAuthConsumer consumer = getConsumer();
                consumer.sign(huc);
                huc.connect(); //not necessary.. because, when you call
//                getContent() on huc it will call connect() internally..but 
//                to be on safe side we are calling it explicitly..
                breader = new BufferedReader(
                        new InputStreamReader(
                                (InputStream)huc.getContent()));
                StringBuilder sb=new StringBuilder();
                String line = "";
                while((line = breader.readLine()) != null)
                    sb.append(line);
                //result is an JSON object with "statuses" as tweets array
                try{
                    JSONArray temp_tweets = new JSONObject(sb.toString())
                                        .getJSONArray("statuses");
                    count = temp_tweets.length();
                    if(since_id == 0) // if it's not set...
                        since_id = ((JSONObject)temp_tweets.get(0)).getLong("id");
                    for(int i=0;i<temp_tweets.length();i++){
                        JSONObject jobj = temp_tweets.getJSONObject(i);
                        tweets.put(jobj);//add it to global tweets
                        // update the max_id value...
                        if(!jobj.isNull("id")) {
                            max_id = jobj.getLong("id");
                        }
                    } // iterated through all tweets and added them to global tweets..
                }catch(JSONException ex){
                    ex.printStackTrace();
                }
                
            } // end of while loop
            
        } catch (URISyntaxException ex) {
            System.out.println("URISyntaxException");
        } catch (MalformedURLException ex) {
            System.out.println("MalformedURLException");
        } catch (IOException ex) {
            System.out.println("IOException");
            System.out.println(ex.getMessage());
        } catch (OAuthMessageSignerException ex) {
            System.out.println("OAuthMessageSignerException..");
        } catch (OAuthExpectationFailedException ex) {
            System.out.println("OAuthExpectationFailedException..");
        } catch (OAuthCommunicationException ex) {
            System.out.println("OAuthCommunicationException..");
        }
        return tweets;
    }
    
    
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter a query to search the twitter..#xyz, @trees..: ");
        String query = sc.nextLine();
        
        System.out.println(new QuerySearch().getTweets(query));
    }
    
}
