package service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.elasticsearch.search.builder.SearchSourceBuilder;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;
import service.Tweet;

import org.elasticsearch.index.query.QueryBuilders;

/**
 * Servlet implementation class GetTweetsElasticSearch
 */
public class GetTweetsElasticSearch extends HttpServlet {
	private static final long serialVersionUID = 1L;
	String elasticSearchURL = "";

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public GetTweetsElasticSearch() {
		super();
	}

	@Override
	public void init() throws ServletException {
		// TODO Auto-generated method stub
		super.init();
		elasticSearchURL = getServletContext().getInitParameter("elasticSearchURL");
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	@SuppressWarnings("deprecation")
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		// response.getWriter().append("Served at:
		// ").append(request.getContextPath());
		// Construct a new Jest client according to configuration via factory
		JestResult result;
		String scrollId = "";

		String searchParameter = request.getParameter("keyword");
		String scrollIdParameter = request.getParameter("scrollId");
		request.setAttribute("keyWord", searchParameter);
		// System.out.println(scrollIdParameter);

		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder(elasticSearchURL).multiThreaded(true).build());
		JestClient client = factory.getObject();

		if (scrollIdParameter == null) {
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.size(5);
			searchSourceBuilder.from(0);

			if (searchParameter != null && !searchParameter.isEmpty())
				searchSourceBuilder.query(QueryBuilders.matchQuery("text", searchParameter));
			else
				searchSourceBuilder.query(QueryBuilders.matchAllQuery());

			Search search = new Search.Builder(searchSourceBuilder.toString())
					// multiple index or types can be added.
					.addIndex("tweetsentiments").addType("tweets").setParameter(Parameters.SCROLL, "2m").build();
			// System.out.println("Executing simple");
			result = client.execute(search);
		} else {
			// System.out.println("Executing scroll");
			SearchScroll scroll = new SearchScroll.Builder(scrollIdParameter, "2m").setParameter(Parameters.SIZE, 5)
					.build();
			result = client.execute(scroll);
		}
		// System.out.println(result.getJsonString());
		scrollId = result.getJsonObject().getAsJsonPrimitive("_scroll_id").getAsString();

		List<Tweet> tweets = result.getSourceAsObjectList(Tweet.class);
		// System.out.println(tweets.size());
		if (tweets.size() > 0) {
			Tweet t = null;
			String text = null;
			PrintWriter pw = response.getWriter();
			pw.write("{\"tweets\":[");
			int i = 0;
			for (i = 0; i < tweets.size() - 1; i++) {
				t = tweets.get(i);
				text = t.getText();

				// System.out.println(text);
				pw.write("{\"lat\": " + t.getLat() + ", \"lng\": " + t.getLng() + ", \"text\":\""
						+ URLEncoder.encode(text, "UTF-8") + "\", \"sentiment\":\"" + t.getSentiment() + "\"}, ");
			}
			t = tweets.get(i);
			text = t.getText();
			pw.write("{\"lat\": " + t.getLat() + ", \"lng\": " + t.getLng() + ", \"text\":\""
					+ URLEncoder.encode(text, "UTF-8") + "\", \"sentiment\":\"" + t.getSentiment() + "\"}");
			pw.write("], \"scrollId\" : \"" + scrollId + "\"}");
			pw.close();
		} else {
			PrintWriter pw = response.getWriter();
			pw.write("{\"tweets\":[");
			pw.write("], \"scrollId\" : \"" + scrollId + "\"}");

			pw.close();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
