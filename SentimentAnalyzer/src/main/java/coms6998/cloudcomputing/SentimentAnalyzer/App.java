package coms6998.cloudcomputing.SentimentAnalyzer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

/**
 * Hello world!
 *
 */
public class App {
	static String queueUrl = null;
	static AmazonSQS sqs = null;
	static ExecutorService executor;
	private static String baseURL = "http://access.alchemyapi.com/calls/text/TextGetTextSentiment";
	private static String key = "724912170947543d2de401b5b4b3707fb329d011";
	private static AWSCredentials credentials;

	public static void main(String[] args) {
		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (~/.aws/credentials).
		 */
		credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("siddharth")
					.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location (~/.aws/credentials), and is in valid format.",
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

		executor = Executors.newFixedThreadPool(5);
		Runnable worker = new WorkerThread();
		executor.execute(worker);
		// executor.shutdown();

		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	static public class WorkerThread implements Runnable {

		public void run() {
			// TODO Auto-generated method stub
			try {
				while (true) {
					processQueue();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		private void processQueue() throws IOException {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
					queueUrl);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest)
					.getMessages();
			System.out.println(messages.size());
			for (Message message : messages) {
	            String messageReceiptHandle = message.getReceiptHandle();
	            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageReceiptHandle));
	            
				JSONObject jsonObject;
				JSONObject response = null;
				try {
					jsonObject = new JSONObject(message.getBody());
				} catch (Exception e) {
					System.out.println("Catch");
					continue;
				}
				String text = jsonObject.getString("text");
				System.out.println(text);

				StringBuilder params = new StringBuilder();
				params.append("apikey=" + key);
				params.append("&text=" + URLEncoder.encode(text,"ASCII"));
				params.append("&outputMode=json");
				params.append("&language=english");
				params.append("&showSourceText=1");

				StringBuilder uri = new StringBuilder();
				uri.append(baseURL).append('?').append(params.toString());

				URL url = new URL(uri.toString());
				HttpURLConnection handle = (HttpURLConnection) url
						.openConnection();
				handle.setDoOutput(true);

				try {
					int status = handle.getResponseCode();
					System.out.println(status);
					switch (status) {
					case 200:
					case 201:
						BufferedReader br = new BufferedReader(
								new InputStreamReader(handle.getInputStream()));
						StringBuilder sb = new StringBuilder();
						String line;
						while ((line = br.readLine()) != null) {
							sb.append(line + "\n");
						}
						System.out.println(sb.toString());
						br.close();
						response = new JSONObject(sb.toString());
					}
				} catch (IOException ex) {
					System.out.println("IO Exception ");
				}
				if(response!=null){
					try{
					String sentiment = response.getJSONObject("docSentiment").getString("type");
					jsonObject.put("sentiment", sentiment);
					String topicArn = "arn:aws:sns:us-west-2:908762746590:tweets";
					AmazonSNSClient snsClient = new AmazonSNSClient(credentials);		                           
					snsClient.setRegion(Region.getRegion(Regions.US_WEST_2));
					PublishRequest publishRequest = new PublishRequest(topicArn, jsonObject.toString());
					PublishResult publishResult = snsClient.publish(publishRequest);
					System.out.println(publishResult.toString());
					} catch(JSONException e){
						System.out.println("Skipped Tweet");
						e.printStackTrace();
					} catch (Exception e) {
						// TODO: handle exception
						e.printStackTrace();
					}
				}else{
					System.out.println("response null");
				}
			}
		}
	}
}
