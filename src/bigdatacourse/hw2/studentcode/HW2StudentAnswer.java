package bigdatacourse.hw2.studentcode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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
	//TODO: add here create table and query designs 
	
	// items table
	public static final String ITEMS_TABLE_CREATE = "CREATE TABLE IF NOT EXISTS items (\n"
			+ "asin text,\n"
			+ "title text,\n"
			+ "image text,\n"
			+ "categories set<text>,\n"
			+ "description text,\n"
			+ "PRIMARY KEY (asin)\n"
			+ ");";
	
	public static final String USER_REVIEW_TABLE_CREATE = "CREATE TABLE IF NOT EXISTS user_reviews (\n"
			+ "reviewerID text,\n"
			+ "ts timestamp,\n"
			+ "reviewerName text,\n"
			+ "asin text,\n"
			+ "rating float,\n"
			+ "summary text,\n"
			+ "reviewText text,\n"
			+ "PRIMARY KEY (reviewerID, ts)\n"
			+ ") \n"
			+ "WITH CLUSTERING ORDER BY (ts DESC);\n";
	
	public static final String ITEM_REVIEW_TABLE_CREATE = "CREATE TABLE IF NOT EXISTS item_reviews (\n"
			+ "asin text,\n"
			+ "ts timestamp,\n"
			+ "reviewerID text,\n"
			+ "reviewerName text,\n"
			+ "rating float,\n"
			+ "summary text,\n"
			+ "reviewText text,\n"
			+ "PRIMARY KEY (asin, ts)\n"
			+ ") \n"
			+ "WITH CLUSTERING ORDER BY (ts DESC);\n";
	
	
	public static final String ITEMS_INSERT = "INSERT INTO items(asin, title, image, categories, description) VALUES (?, ?, ?, ?, ?)";
	public static final String ITEMS_SELECT = "SELECT asin, title, image, categories, description FROM items WHERE asin = (?)";
	public static final String USER_REVIEW_INSERT = "INSERT INTO user_reviews(reviewerid, ts, reviewerName, asin, rating, summary, reviewText) VALUES (?, ?, ?, ?, ?, ?, ?)";
	public static final String USER_REVIEW_SELECT = "SELECT ts, asin, reviewerid, reviewername, rating, summary, reviewtext FROM user_reviews WHERE reviewerid = (?)";
	public static final String ITEM_REVIEW_INSERT = "INSERT INTO item_reviews(asin, ts, reviewerid, reviewerName, rating, summary, reviewText) VALUES (?, ?, ?, ?, ?, ?, ?)";
	public static final String ITEM_REVIEW_SELECT = "SELECT ts, asin, reviewerid, reviewername, rating, summary, reviewtext FROM item_reviews WHERE asin = (?)";

	
	// cassandra session
	private CqlSession session;
	
	// prepared statements
	//TODO: add here prepared statements variables
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
		
		// sync
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
		
		// sync
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
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		session.execute(ITEMS_TABLE_CREATE);
		session.execute(USER_REVIEW_TABLE_CREATE);
		session.execute(ITEM_REVIEW_TABLE_CREATE);
		
	}

	@Override
	public void initialize() {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		pItemInsert = session.prepare(ITEMS_INSERT);
		pItemSelect = session.prepare(ITEMS_SELECT);
		
		pUserReviewInsert = session.prepare(USER_REVIEW_INSERT);
		pUserReviewSelect = session.prepare(USER_REVIEW_SELECT);
		
		pItemReviewInsert = session.prepare(ITEM_REVIEW_INSERT);
		pItemReviewSelect = session.prepare(ITEM_REVIEW_SELECT);
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
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
		
		while ((line = br.readLine()) != null) {
			count++;
			System.out.print(count + ": ");
			categories.clear();
			sb.append(line);
			System.out.println(sb.toString());
			item = new JSONObject(sb.toString());
//			System.out.println(item.toString(4));
			
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
			
			JSONArray cats = ((JSONArray) item.get("categories")).getJSONArray(0);
						
			for (int i = 0 ; i < cats.length(); i++) {
				categories.add(cats.get(i).toString());
			}
//			System.out.println(categories);
			
			insertItem(pItemInsert, asin, title, image, categories, description);
			
			if (count % 3000 == 0) {
				TimeUnit.SECONDS.sleep(1);
			}	
			sb.setLength(0);
		}
	}
	
	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		JSONObject item = new JSONObject();
		
		File file = new File(pathReviewsFile);
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		StringBuffer sb = new StringBuffer();
		System.out.println(sb.toString());
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
			System.out.print(count + ": ");
			sb.append(line);
			System.out.println(sb.toString());
			item = new JSONObject(sb.toString());
			System.out.println(item.toString());
			
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
			
			System.out.println("time: " + ts);
			System.out.println(" ,asin: " + asin);
			System.out.println(" ,reveiwerID: " + reviewerID);
			System.out.println(" ,reviewerName: " + reviewerName);
			System.out.println(" ,rating: " + rating);
			System.out.println(" ,summary: " + summary);
			System.out.println(" ,reviewText: " + reviewText);
			
			insertUserReview(pUserReviewInsert, reviewerID, ts, reviewerName, asin, rating, summary, reviewText);
			insertItemReview(pItemReviewInsert, asin, ts, reviewerID, reviewerName, rating, summary, reviewText);
			
			if (count % 1500 == 0) {
				TimeUnit.SECONDS.sleep(1);
			}	
			sb.setLength(0);
		}
		
	}

	@Override
	public void item(String asin) {
//		//TODO: implement this function
//		System.out.println("TODO: implement this function...");
//		
//		// required format - example for asin B005QB09TU
//		System.out.println("asin: " 		+ "B005QB09TU");
//		System.out.println("title: " 		+ "Circa Action Method Notebook");
//		System.out.println("image: " 		+ "http://ecx.images-amazon.com/images/I/41ZxT4Opx3L._SY300_.jpg");
//		System.out.println("categories: " 	+ new HashSet<String>(Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper")));
//		System.out.println("description: " 	+ "Circa + Behance = Productivity. The minute-to-minute flexibility of Circa note-taking meets the organizational power of the Action Method by Behance. The result is enhanced productivity, so you'll formulate strategies and achieve objectives even more efficiently with this Circa notebook and project planner. Read Steve's blog on the Behance/Levenger partnership Customize with your logo. Corporate pricing available. Please call 800-357-9991.");;
//		
//		// required format - if the asin does not exists return this value
//		System.out.println("not exists");
		
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
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		
//		// required format - example for reviewerID A17OJCRPMYWXWV
//		System.out.println(	
//				"time: " 			+ Instant.ofEpochSecond(1362614400) + 
//				", asin: " 			+ "B005QDG2AI" 	+
//				", reviewerID: " 	+ "A17OJCRPMYWXWV" 	+
//				", reviewerName: " 	+ "Old Flour Child"	+
//				", rating: " 		+ 5 	+ 
//				", summary: " 		+ "excellent quality"	+
//				", reviewText: " 	+ "These cartridges are excellent .  I purchased them for the office where I work and they perform  like a dream.  They are a fraction of the price of the brand name cartridges.  I will order them again!");
//
//		System.out.println(	
//				"time: " 			+ Instant.ofEpochSecond(1360108800) + 
//				", asin: " 			+ "B003I89O6W" 	+
//				", reviewerID: " 	+ "A17OJCRPMYWXWV" 	+
//				", reviewerName: " 	+ "Old Flour Child"	+
//				", rating: " 		+ 5 	+ 
//				", summary: " 		+ "Checkbook Cover"	+
//				", reviewText: " 	+ "Purchased this for the owner of a small automotive repair business I work for.  The old one was being held together with duct tape.  When I saw this one on Amazon (where I look for almost everything first) and looked at the price, I knew this was the one.  Really nice and very sturdy.");
//
//		System.out.println("total reviews: " + 2);
		
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
			System.out.print(", rating: " + row.getFloat(4));
			System.out.print(", summary: " + row.getString(5));
			System.out.println(", reviewText: " + row.getString(6));
			count++;
			row = rs.one();
		}
		
		System.out.println("total reviews: " + count);
		
	}

	@Override
	public void itemReviews(String asin) {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		
//		// required format - example for asin B005QDQXGQ
//		System.out.println(	
//				"time: " 			+ Instant.ofEpochSecond(1391299200) + 
//				", asin: " 			+ "B005QDQXGQ" 	+
//				", reviewerID: " 	+ "A1I5J5RUJ5JB4B" 	+
//				", reviewerName: " 	+ "T. Taylor \"jediwife3\""	+
//				", rating: " 		+ 5 	+ 
//				", summary: " 		+ "Play and Learn"	+
//				", reviewText: " 	+ "The kids had a great time doing hot potato and then having to answer a question if they got stuck with the &#34;potato&#34;. The younger kids all just sat around turnin it to read it.");
//
//		System.out.println(	
//				"time: " 			+ Instant.ofEpochSecond(1390694400) + 
//				", asin: " 			+ "B005QDQXGQ" 	+
//				", reviewerID: " 	+ "AF2CSZ8IP8IPU" 	+
//				", reviewerName: " 	+ "Corey Valentine \"sue\""	+
//				", rating: " 		+ 1 	+ 
//				", summary: " 		+ "Not good"	+
//				", reviewText: " 	+ "This Was not worth 8 dollars would not recommend to others to buy for kids at that price do not buy");
//
//		System.out.println(	
//				"time: "			+ Instant.ofEpochSecond(1388275200) + 
//				", asin: " 			+ "B005QDQXGQ" 	+
//				", reviewerID: " 	+ "A27W10NHSXI625" 	+
//				", reviewerName: " 	+ "Beth"	+
//				", rating: " 		+ 2 	+ 
//				", summary: " 		+ "Way overpriced for a beach ball"	+
//				", reviewText: " 	+ "It was my own fault, I guess, for not thoroughly reading the description, but this is just a blow-up beach ball.  For that, I think it was very overpriced.  I thought at least I was getting one of those pre-inflated kickball-type balls that you find in the giant bins in the chain stores.  This did have a page of instructions for a few different games kids can play.  Still, I think kids know what to do when handed a ball, and there's a lot less you can do with a beach ball than a regular kickball, anyway.");
//
//		System.out.println("total reviews: " + 3);
		
		
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
			System.out.print(", rating: " + row.getFloat(4));
			System.out.print(", summary: " + row.getString(5));
			System.out.println(", reviewText: " + row.getString(6));
			count++;
			row = rs.one();
		}
		
		System.out.println("total reviews: " + count);
		
	}


}
