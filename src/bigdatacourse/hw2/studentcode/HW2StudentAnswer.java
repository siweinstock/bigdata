package bigdatacourse.hw2.studentcode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import bigdatacourse.hw2.HW2API;

public class HW2StudentAnswer implements HW2API{
	
	// general consts
	public static final String		NOT_AVAILABLE_VALUE 	=		"na";

	// CQL stuff
	
	private static final String		TABLE_ITEMS = "items";
	private static final String		TABLE_ITEM_REVIEWS = "item_reviews";
	private static final String		TABLE_USER_REVIEWS = "user_reviews";
	
	
	public static final String ITEMS_TABLE_CREATE = "CREATE TABLE IF NOT EXISTS " + TABLE_ITEMS + " (\n"
			+ "asin text,\n"
			+ "title text,\n"
			+ "image text,\n"
			+ "categories set<text>,\n"
			+ "description text,\n"
			+ "PRIMARY KEY (asin)\n"
			+ ");";
	
	public static final String USER_REVIEW_TABLE_CREATE = "CREATE TABLE IF NOT EXISTS " + TABLE_USER_REVIEWS + " (\n"
			+ "reviewerID text,\n"
			+ "ts timestamp,\n"
			+ "reviewerName text,\n"
			+ "asin text,\n"
			+ "rating float,\n"
			+ "summary text,\n"
			+ "reviewText text,\n"
			+ "PRIMARY KEY ((reviewerID), ts, asin)\n"
			+ ") \n"
			+ "WITH CLUSTERING ORDER BY (ts DESC, asin ASC);\n";
	
	public static final String ITEM_REVIEW_TABLE_CREATE = "CREATE TABLE IF NOT EXISTS " + TABLE_ITEM_REVIEWS + " (\n"
			+ "asin text,\n"
			+ "ts timestamp,\n"
			+ "reviewerID text,\n"
			+ "reviewerName text,\n"
			+ "rating float,\n"
			+ "summary text,\n"
			+ "reviewText text,\n"
			+ "PRIMARY KEY ((asin), ts, reviewerID)\n"
			+ ") \n"
			+ "WITH CLUSTERING ORDER BY (ts DESC, reviewerID ASC);\n";
	
	
	public static final String ITEMS_INSERT = "INSERT INTO " + TABLE_ITEMS + "(asin, title, image, categories, description) VALUES (?, ?, ?, ?, ?)";
	public static final String ITEMS_SELECT = "SELECT asin, title, image, categories, description FROM " + TABLE_ITEMS + " WHERE asin = (?)";
	
	public static final String USER_REVIEW_INSERT = "INSERT INTO " + TABLE_USER_REVIEWS + "(reviewerid, ts, reviewerName, asin, rating, summary, reviewText) VALUES (?, ?, ?, ?, ?, ?, ?)";
	public static final String USER_REVIEW_SELECT = "SELECT ts, asin, reviewerid, reviewername, rating, summary, reviewtext FROM " + TABLE_USER_REVIEWS + " WHERE reviewerid = (?)";
	
	public static final String ITEM_REVIEW_INSERT = "INSERT INTO " + TABLE_ITEM_REVIEWS + "(asin, ts, reviewerid, reviewerName, rating, summary, reviewText) VALUES (?, ?, ?, ?, ?, ?, ?)";
	public static final String ITEM_REVIEW_SELECT = "SELECT ts, asin, reviewerid, reviewername, rating, summary, reviewtext FROM " + TABLE_ITEM_REVIEWS + " WHERE asin = (?)";

	
	// cassandra session
	private CqlSession session;
	
	// prepared statements
	PreparedStatement pItemInsert;
	PreparedStatement pItemSelect;
	
	PreparedStatement pUserReviewInsert;
	PreparedStatement pUserReviewSelect;
	
	PreparedStatement pItemReviewInsert;
	PreparedStatement pItemReviewSelect;
	
	
	// insert one item
	private void insertItem(PreparedStatement pstmt, String asin, String title, String image, Set<String> categories, String description) {
		BoundStatement bstmt = pItemInsert.bind()
				.setString(0, asin)
				.setString(1, title)
				.setString(2, image)
				.setSet(3, categories, String.class)	// we know set<text> is not recommended but we used it since the # of cat is bounded and not expected to change often (if at all)
				.setString(4, description);
		
		// async
		session.executeAsync(bstmt);
	}
	
	
	// insert one user review
	private void insertUserReview(PreparedStatement pstmt, String reviewerID, long ts, String reviewerName, String asin, float rating, String summary, String reviewText) {
		BoundStatement bstmt = pUserReviewInsert.bind()
				.setString(0, reviewerID)
				.setInstant(1, Instant.ofEpochSecond(ts))
				.setString(2, reviewerName)
				.setString(3, asin)
				.setFloat(4, rating)
				.setString(5, summary)
				.setString(6, reviewText);
		
		// async
		session.executeAsync(bstmt);
	}
	
	
	// insert one item review
	private void insertItemReview(PreparedStatement pstmt, String asin, long ts, String reviewerID, String reviewerName, float rating, String summary, String reviewText) {
		BoundStatement bstmt = pItemReviewInsert.bind()
				.setString(0, asin)
				.setInstant(1, Instant.ofEpochSecond(ts))
				.setString(2, reviewerID)
				.setString(3, reviewerName)
				.setFloat(4, rating)
				.setString(5, summary)
				.setString(6, reviewText);
		
		// async
		session.executeAsync(bstmt);
	}
	
	
	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}
		
		System.out.println("Initializing connection to Cassandra...");
		
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();
		
		System.out.println("Initializing connection to Cassandra... Done");
	}


	@Override
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}
		
		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	
	@Override
	public void createTables() {
		System.out.print("Creating tables ... ");
		
		session.execute(ITEMS_TABLE_CREATE);
		session.execute(USER_REVIEW_TABLE_CREATE);
		session.execute(ITEM_REVIEW_TABLE_CREATE);	
		
		System.out.println("DONE.");
	}

	
	@Override
	public void initialize() {
		System.out.print("Preparing statements ... ");
		
		pItemInsert = session.prepare(ITEMS_INSERT);
		pItemSelect = session.prepare(ITEMS_SELECT);
		
		pUserReviewInsert = session.prepare(USER_REVIEW_INSERT);
		pUserReviewSelect = session.prepare(USER_REVIEW_SELECT);
		
		pItemReviewInsert = session.prepare(ITEM_REVIEW_INSERT);
		pItemReviewSelect = session.prepare(ITEM_REVIEW_SELECT);
		
		System.out.println("DONE.");
	}

	
	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		System.out.print("Loading items");
		
		JSONObject item = new JSONObject();
		File file = new File(pathItemsFile);
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		StringBuffer sb = new StringBuffer();
		String line;
		
		Set<String> categories = new HashSet<>();
		String asin;
		String title;
		String image;
		String description;
		int count = 0;
		int catIn;
		
		while ((line = br.readLine()) != null) {
			count++;
//			System.out.print(count + ": ");
			categories.clear();
			sb.append(line);
//			System.out.println(sb.toString());
			item = new JSONObject(sb.toString());
			asin = item.getString("asin"); // UID must be present, no need for try block
			
			try {
				title = item.getString("title");
			}
			catch (org.json.JSONException e) {
				title = NOT_AVAILABLE_VALUE;
			}
			
			try {
				image = item.getString("imUrl");
			}
			catch (org.json.JSONException e) {
				image = NOT_AVAILABLE_VALUE;
			}
			
			try {
				description = item.getString("description");
			}
			catch (org.json.JSONException e) {
				description = NOT_AVAILABLE_VALUE;
			}
			
			JSONArray cats; //= ((JSONArray) item.get("categories")).getJSONArray(0);
			catIn = 0;
			
			// get all lists of categories and "flatten"
			while (true) {
				try {
					cats = ((JSONArray) item.get("categories")).getJSONArray(catIn);
					catIn++;
					
					for (int i = 0 ; i < cats.length(); i++) {
						categories.add(cats.get(i).toString());
					}
				}
				catch (org.json.JSONException e) {
					break;
				}				
			}

			insertItem(pItemInsert, asin, title, image, categories, description);
			
			// avoid reaching astra's limit
			if (count % 3000 == 0) {
				System.out.print(".");
				TimeUnit.SECONDS.sleep(1);
			}	
			
			sb.setLength(0);
		}
		
		System.out.println("DONE.");
	}
	
	
	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		System.out.print("Loading reviews");
		
		JSONObject item = new JSONObject();
		
		File file = new File(pathReviewsFile);
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		StringBuffer sb = new StringBuffer();
//		System.out.println(sb.toString());
		String line;
		
		String asin;
		String reviewerID;
		String reviewerName;
		float rating;
		long ts;
		String summary;
		String reviewText;
		int count = 0;
		
		while ((line = br.readLine()) != null) {
			count++;
//			System.out.print(count + ": ");
			sb.append(line);
//			System.out.println(sb.toString());
			item = new JSONObject(sb.toString());
//			System.out.println(item.toString());
			
			asin = item.getString("asin");
			reviewerID = item.getString("reviewerID");
			try {
				reviewerName = item.getString("reviewerName");
			}
			catch (org.json.JSONException e) {
				reviewerName = NOT_AVAILABLE_VALUE;
			}
			
			rating = (float)item.getDouble("overall");
			ts = item.getLong("unixReviewTime");
			summary = item.getString("summary");
			reviewText = item.getString("reviewText");
			
			insertUserReview(pUserReviewInsert, reviewerID, ts, reviewerName, asin, rating, summary, reviewText);
			insertItemReview(pItemReviewInsert, asin, ts, reviewerID, reviewerName, rating, summary, reviewText);
			
			// avoid reaching astra's limit
			if (count % 1500 == 0) {
				System.out.print(".");
				TimeUnit.SECONDS.sleep(1);
			}
			
			sb.setLength(0);
		}
		
		System.out.println("DONE.");
	}

	
	@Override
	public void item(String asin) {
		BoundStatement bstmt = pItemSelect.bind().setString(0, asin);
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		
		if (row == null) {
			System.out.println("not exists");
		}
		
		while (row != null) {
			System.out.println("asin: " + row.getString(0));
			System.out.println("title: " + row.getString(1));
			System.out.println("image: " + row.getString(2));
			System.out.println("categories: " + row.getSet(3, String.class));
			System.out.println("description: " + row.getString(4));
			row = rs.one();
		}
	}
	
	
	@Override
	public void userReviews(String reviewerID) {
		BoundStatement bstmt = pUserReviewSelect.bind().setString(0, reviewerID);
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		int count = 0;
		
		if (row == null) {
			System.out.println("not exists");
		}
		
		while (row != null) {
			System.out.print("time: " + row.getInstant(0));
			System.out.print(", asin: " + row.getString(1));
			System.out.print(", reviewerID: " + row.getString(2));
			System.out.print(", reviewerName: " + row.getString(3));
			System.out.print(", rating: " + (int)row.getFloat(4));
			System.out.print(", summary: " + row.getString(5));
			System.out.println(", reviewText: " + row.getString(6));
			count++;
			row = rs.one();
		}
		
		System.out.println("total reviews: " + count);
		
	}

	
	@Override
	public void itemReviews(String asin) {		
		BoundStatement bstmt = pItemReviewSelect.bind().setString(0, asin);
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		int count = 0;
		
		if (row == null) {
			System.out.println("not exists");
		}
		
		while (row != null) {
			System.out.print("time: " + row.getInstant(0));
			System.out.print(", asin: " + row.getString(1));
			System.out.print(", reviewerID: " + row.getString(2));
			System.out.print(", reviewerName: " + row.getString(3));
			System.out.print(", rating: " + (int)row.getFloat(4));
			System.out.print(", summary: " + row.getString(5));
			System.out.println(", reviewText: " + row.getString(6));
			count++;
			row = rs.one();
		}
		
		System.out.println("total reviews: " + count);
		
	}


}
