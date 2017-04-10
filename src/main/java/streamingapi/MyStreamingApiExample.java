
package streamingapi;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import oauth.signpost.OAuthConsumer;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import support.OAuthTokenSecret;
import utils.OAuthUtils;

public class MyStreamingApiExample {
    OAuthTokenSecret OAuthToken;
    final int RECORDS_TO_PROCESS = 10;
    HashSet<String> Keywords;
    HashSet<String> Geoboxes;
    HashSet<String> Userids;
//    final String CONFIG_FILE_PATH = "/home/satheesh/projects/TDAnalysis/src/main/java/streamingapi/searchKeywords.txt";
//    final String DEF_OUTPATH = "/home/satheesh/projects/TDAnalysis/src/main/java/streamingapi/";
    final String CONFIG_FILE_PATH = "/home/ram/NetBeansProjects/MyTDAnalysis/src/main/java/streamingapi/searchKeywords.txt";
    final String DEF_OUTPATH = "/home/ram/NetBeansProjects/MyTDAnalysis/src/main/java/streamingapi/tweets/";
    
    final String USERS_FILE_PATH ="/home/ram/NetBeansProjects/MyTDAnalysis/src/main/java/streamingapi/user_ids.txt";
    final String LOCATIONS_PATH = "/home/ram/NetBeansProjects/MyTDAnalysis/src/main/java/streamingapi/geo.txt";
    /**
     * Loads the Twitter access token and secret for a user
     */
    public void LoadTwitterToken() {
        OAuthToken = new OAuthTokenSecret(OAuthUtils.ACCESS_TOKEN, OAuthUtils.ACCESS_TOKEN_SECRET);
    }

    public void createStreamingConnection(String baseUrl, String outFilePath) {
        HttpClient httpClient = new DefaultHttpClient();
        httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, new Integer(90000));
        OAuthConsumer consumer = new CommonsHttpOAuthConsumer(OAuthUtils.CONSUMER_KEY,OAuthUtils.CONSUMER_SECRET);
        consumer.setTokenWithSecret(OAuthToken.getAccessToken(),OAuthToken.getAccessSecret());
        HttpPost httppost = new HttpPost(baseUrl);
        try {
            httppost.setEntity(new UrlEncodedFormEntity(createRequestBody(), "UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            ex.printStackTrace();
        }
        try {
             //Step 3: Sign the request
                consumer.sign(httppost);
        } catch (Exception ex) {
                ex.printStackTrace();
        }
        try {
            System.out.println("generated body of url:"+EntityUtils.toString(httppost.getEntity()));
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        HttpResponse response;
        InputStream is = null;
        try {
             //Step 4: Connect to the API
                response = httpClient.execute(httppost);
                if (response.getStatusLine().getStatusCode()!= HttpStatus.SC_OK)
                {
                    throw new IOException("Got status " +response.getStatusLine().getStatusCode());
                }
                else {
                    System.out.println(OAuthToken.getAccessToken()+ ": Processing from " + baseUrl);
                    HttpEntity entity = response.getEntity();
                    try {
                        is = entity.getContent();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    } catch (IllegalStateException ex) {
                        ex.printStackTrace();
                    }
                    //Step 5: Process the incoming Tweet Stream
                    this.processTwitterStream(is, outFilePath);
                }
         } catch (IOException ex) {
            ex.printStackTrace();
        }finally {
            // Abort the method, otherwise releaseConnection() will
            // attempt to finish reading the never-ending response.
            // These methods do not throw exceptions. 
            if(is!=null)
            {
                try {
                    is.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public void processTwitterStream(InputStream is, String outFilePath) {
        BufferedWriter bwrite = null;
        try {
            JSONTokener jsonTokener = new JSONTokener(new InputStreamReader(is, "UTF-8"));
            ArrayList<JSONObject> rawtweets = new ArrayList<JSONObject>();
            int nooftweetsuploaded = 0;
            while (true) {
                // just fetch 20 tweets and then stops.. just to show you the output...
                if(nooftweetsuploaded > 20)
                    break;
                try {
                    JSONObject temp = new JSONObject(jsonTokener);
                    rawtweets.add(temp);
                    if (rawtweets.size() >= RECORDS_TO_PROCESS)
                    {
                        Calendar cal = Calendar.getInstance();
                        String filename = outFilePath + "tweets_" + cal.getTimeInMillis() + ".json";
                        bwrite = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), "UTF-8"));
                        nooftweetsuploaded += RECORDS_TO_PROCESS;
                        //Write the collected tweets to a file
                        for (JSONObject jobj : rawtweets) {
                            bwrite.write(jobj.toString());
                            bwrite.newLine();
                        }
                        System.out.println("Written "+nooftweetsuploaded+" records so far");
                        bwrite.close();
                        rawtweets.clear();
                    }
                } catch (JSONException ex) {
                    ex.printStackTrace();
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        MyStreamingApiExample sae = new MyStreamingApiExample();
        sae.LoadTwitterToken();
        //load parameters from a TSV file
        String filename = sae.CONFIG_FILE_PATH;
        String outfilepath = sae.DEF_OUTPATH;
        sae.readParameters(filename);

        String userfilepath =sae.USERS_FILE_PATH;
        sae.readUsers(userfilepath);
        String location_file_path = sae.LOCATIONS_PATH;
        sae.readLocations(location_file_path);
        
        sae.createStreamingConnection("https://stream.twitter.com/1.1/statuses/filter.json", outfilepath);
    }
    
    
    public void readUsers(String filename) throws IOException{
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(filename), "UTF-8"));
            if(Userids == null)
                Userids = new HashSet<String>();
            String temp ="";
             while ((temp = br.readLine()) != null) {
                String[] users = temp.split(" ");
                for (String word : users) {
                    if (!Userids.contains(word)) {
                        Userids.add(word);
                    }
                }
            }
        } finally{
            try {
                br.close();
            } finally{
                
            }
        }
    }
    
    public void readLocations(String filename) throws IOException{
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(filename), "UTF-8"));
            if(Geoboxes == null)
                Geoboxes = new HashSet<String>();
            String temp ="";
             while ((temp = br.readLine()) != null) {
                String[] users = temp.split(" ");
                for (String word : users) {
                    if (!Geoboxes.contains(word)) {
                        Geoboxes.add(word);
                    }
                }
            }
        } finally{
            try {
                br.close();
            } finally{
                
            }
        }
    }
    
    
    public void readParameters(String filename) throws IOException {

        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"));
            String temp = "";
            int count = 1;
            if (Keywords == null) {
                Keywords = new HashSet<String>();
            }
            while ((temp = br.readLine()) != null) {
                String[] keywords = temp.split(" ");
                for (String word : keywords) {
                    if (!Keywords.contains(word)) {
                        Keywords.add(word);
                    }
                }
            }
        } finally {

        }
    }

     private List<NameValuePair> createRequestBody() {
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        if (Keywords != null&&Keywords.size()>0) {
            params.add(createNameValuePair("track", Keywords));
            
            params.add(createNameValuePair("follow", Userids));
            params.add(createNameValuePair("locations", Geoboxes));
        }
        return params;
    }


    private NameValuePair createNameValuePair(String name, Collection<String> items) {
        StringBuilder sb = new StringBuilder();
        boolean needComma = false;
        for (String item : items) {
            if (needComma) {
                sb.append(',');
            }
            needComma = true;
            sb.append(item);
        }
        return new BasicNameValuePair(name, sb.toString());
    }
}
