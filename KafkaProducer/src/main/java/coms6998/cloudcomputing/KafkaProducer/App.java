package coms6998.cloudcomputing.KafkaProducer;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

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
	static String topicName = "tweetsentiments";

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		final Producer<String, String> producer = new KafkaProducer<String, String>(
				props);

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
				for (char c : s.toCharArray()) {
					if (c < 32 || c > 126)
						return false;
				}
				return true;
			}

			public void onStatus(Status status) {
				if (status.getGeoLocation() != null
						&& isEnglish(status.getText())) {
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
					System.out.println("Sent:" + jsonObject);
					String tweet = new String(jsonObject.toString().getBytes(
							Charset.forName("US-ASCII")));
					System.out.println("\nTweet:" + tweet);
					ProducerRecord<String, String> record = new ProducerRecord<String, String>(
							topicName, tweet);
					producer.send(record);
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
		fq.language(new String[] { "en" });
		double locations[][] = { { -180, -90 }, { 180, 90 } };
		fq.locations(locations);
		twitterStream.filter(fq);

	}
}
