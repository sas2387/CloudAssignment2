package service;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json.JSONObject;

public class TweetIndexer extends HttpServlet {

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		// TODO Auto-generated method stub

		String body = null;
		StringBuilder stringBuilder = new StringBuilder();
		BufferedReader bufferedReader = null;

		try {
			InputStream inputStream = req.getInputStream();
			if (inputStream != null) {
				bufferedReader = new BufferedReader(new InputStreamReader(
						inputStream));
				char[] charBuffer = new char[128];
				int bytesRead = -1;
				while ((bytesRead = bufferedReader.read(charBuffer)) > 0) {
					stringBuilder.append(charBuffer, 0, bytesRead);
				}
			} else {
				stringBuilder.append("");
			}
		} catch (IOException ex) {
			throw ex;
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException ex) {
					throw ex;
				}
			}
		}

		body = stringBuilder.toString();
		System.out.println(body);

		try {
			JSONObject bodyJson = new JSONObject(body);
			JSONObject tweetDetails = new JSONObject(
					bodyJson.getString("Message"));
			Tweet tweet = new Tweet();
			tweet.setId(tweetDetails.getString("id"));
			tweet.setSentiment(tweetDetails.getString("sentiment"));
			tweet.setTime(new Date(tweetDetails.getString("time")));
			tweet.setText(tweetDetails.getString("text"));
			tweet.setUser(tweetDetails.getString("user"));
			tweet.setLng(tweetDetails.getDouble("lng"));
			tweet.setLat(tweetDetails.getDouble("lat"));

			System.out.println("Sending:" + tweet);
			// Send tweets to Elastic Search
			JestClientFactory factory = new JestClientFactory();
			factory.setHttpClientConfig(new HttpClientConfig.Builder(
					"https://search-tweetsentiments-jg27mbmxojtxfduhcceqqkwkwa.us-east-1.es.amazonaws.com:443")
					.multiThreaded(true).build());
			final JestClient client = factory.getObject();

			Index index = new Index.Builder(tweet).index("tweetsentiments")
					.type("tweets").id(tweet.getId()).build();
			client.execute(index);
		} catch (Exception e) {
			System.out.println("Not a tweet");
			e.printStackTrace();
		}
	}

}
