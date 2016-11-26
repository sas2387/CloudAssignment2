package coms6998.cloudcomputing.SentimentAnalyzer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;

/**
 * Hello world!
 *
 */
public class App 
{
	static String queueUrl = null;
	static AmazonSQS sqs = null;
	static ExecutorService executor;
    public static void main( String[] args )
    {
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
		
    	executor = Executors.newFixedThreadPool(5);	
    	Runnable worker = new WorkerThread();
		executor.execute(worker);
		executor.shutdown();
		
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			  e.printStackTrace();
		}
		
    }
    
    static public class WorkerThread implements Runnable {

	    private void processQueue() {
	        
	    }

		public void run() {
			// TODO Auto-generated method stub
			processQueue();
		}
	}
}
