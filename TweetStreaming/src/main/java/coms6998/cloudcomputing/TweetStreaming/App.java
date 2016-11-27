package coms6998.cloudcomputing.TweetStreaming;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map.Entry;

import org.json.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Hello world!
 *
 */
public class App {
	
	static String queueUrl = null;
	static AmazonSQS sqs = null;
	
	public static void main(String[] args) {
		
		/*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("siddharth").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        sqs = new AmazonSQSClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SQS");
        System.out.println("===========================================\n");

        try {
            // Create a queue
            queueUrl = sqs.listQueues("TweetQueue").getQueueUrls().get(0);
            System.out.println(queueUrl);
        } catch (Exception e) {
        	e.printStackTrace();
        }
		
		

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey("ErraBDFjBgl7kqno0iusPcV8Y");
		cb.setOAuthConsumerSecret("R2Kl1eATAIWghdtHtCvDLBnWHW0b3vrZr6FGW7fJQlIYN96gJc");
		cb.setOAuthAccessToken("602555213-7W3YNu4FBgyYR39liAOUJhhbj73vFObQAWDQelXU");
		cb.setOAuthAccessTokenSecret("tZjLT82nJI1nUUrVhfN9XYyEBSmmIYdsbuG4cUvJHjDDf");

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
				.getInstance();

		StatusListener statusListener = new StatusListener() {

			public void onException(Exception arg0) {
				// TODO Auto-generated method stub

			}

			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub
			}
			
			private boolean isEnglish(String s) {
				for(char c:s.toCharArray()){
					if(c < 32 || c > 126)
						return false;
				}
				return true;
			}

			public void onStatus(Status status) {
				if (status.getGeoLocation() != null && isEnglish(status.getText())) {
					Tweet newTweet = new Tweet();
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");
					String time = sdf.format(status.getCreatedAt());
					String statusstring = status.getText().replaceAll("'",
							"\\\\'");
					statusstring = statusstring.replaceAll("\\s+", " ");

					newTweet.setLat(status.getGeoLocation().getLatitude());
					newTweet.setLng(status.getGeoLocation().getLongitude());
					newTweet.setText(statusstring);
					newTweet.setTime(status.getCreatedAt());
					newTweet.setId(String.valueOf(status.getId()));
					newTweet.setUser(status.getUser().getScreenName());

					JSONObject jsonObject = new JSONObject(newTweet);
					System.out.println("Sent:"+jsonObject);
					sqs.sendMessage(new SendMessageRequest(queueUrl, jsonObject.toString()));
				}
			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub

			}

			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub

			}
		};

		twitterStream.addListener(statusListener);
		FilterQuery fq = new FilterQuery();
		fq.language(new String[]{"en"});
		double locations[][] = { { -180, -90 }, { 180, 90 } };
		fq.locations(locations);
		twitterStream.filter(fq);

		//twitterStream.sample();
	}

}
