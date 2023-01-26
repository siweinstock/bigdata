package bigdatacourse.hw2.studentcode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        int maxThreads	= 250;
        long c=0;
        
        String asin;
        String desc;
        String title;
        String img;
        JSONObject json;
        Set<String> categories = new HashSet<>();
        
        int count = 0;
		int catIn;
        
        // creating the thread factors
        ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
        BoundStatement bstmt;
        File file = new File(pathItemsFile);
		try {
			String content = new String(Files.readAllBytes(Paths.get(file.toURI())));
			for (String row : content.split("\n")){
			    c++;
			    final long x = c;
			    
				json = new JSONObject(row);
				
				asin = json.getString("asin"); // UID must be present, no need for try block
                
    			JSONArray cats; //= ((JSONArray) item.get("categories")).getJSONArray(0);
    			catIn = 0;
                
    			while (true) {
    				try {
    					cats = (JSONArray) ((JSONArray) json.get("categories")).get(catIn);
    					catIn++;
    					
    					for (int i = 0 ; i < cats.length(); i++) {
    						categories.add(cats.get(i).toString());
    					}
    				}
    				catch (org.json.JSONException e) {
    					break;
    				}				
    			}
    			
                try {
                	img = json.getString("imUrl");
                }
                catch (Exception e){
                	img = NOT_AVAILABLE_VALUE;
                }
                
                try {
                	title = json.getString("title");
                }
                catch (Exception e){
                	title = NOT_AVAILABLE_VALUE;
                }
                
                try {
                	desc = json.getString("description");
                }
                catch (Exception e){
                	desc = NOT_AVAILABLE_VALUE;
                }
                
                
        		bstmt = pItemInsert.bind()
        				.setString(0, asin)
        				.setString(1, title)
        				.setString(2, img)
        				.setSet(3, categories, String.class)
        				.setString(4, desc);
                

                final BoundStatement bstmt1 = bstmt;
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        session.execute(bstmt1);
                        System.out.println("version 3 - added " + x);
                    }
                });
                
                categories.clear();
                
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
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
	public void loadReviews(String pathReviewsFile) throws Exception {
		// we tried to go over the file once and insert into both tables, but it caused errors and this is the compromise
		loadReviewsByItem(pathReviewsFile);
		loadReviewsByUser(pathReviewsFile);
		System.out.println("DONE.");
	}
	
	
	private void loadReviewsByItem(String pathReviewsFile) throws Exception {
        int maxThreads	= 250;
        long c=0;

        // creating the thread factors
        ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
        BoundStatement bstmt;

        JSONObject json;
        String rname;
        String summary;
        String reviewText;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(pathReviewsFile));
            String line = reader.readLine();
            while (line != null) {
                c++;
                final long x = c;
                json = new JSONObject(line);

                try {
                	rname = json.getString("reviewerName");
                }
                catch (Exception e){
                	rname = NOT_AVAILABLE_VALUE;
                }
                
                try {
                	summary = json.getString("summary");
                }
                catch (Exception e){
                	summary = NOT_AVAILABLE_VALUE;
                }
                
                try {
                	reviewText = json.getString("reviewText");
                }
                catch (Exception e){
                	reviewText = NOT_AVAILABLE_VALUE;
                }
                
                bstmt = pItemReviewInsert.bind()
                		.setString(0, json.getString("asin"))
                        .setInstant(1, Instant.ofEpochSecond(json.getLong("unixReviewTime")))
                        .setString(2, json.getString("reviewerID"))
                        .setString(3, rname)
                        .setFloat(4, (float)json.getDouble("overall"))
                        .setString(5, summary)
                        .setString(6, reviewText);
                final BoundStatement bound = bstmt;
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        session.execute(bound);
                        System.out.println("ReviewsByItem - added " + x);
                    }
                });
                
                line = reader.readLine();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
	}

	
	private void loadReviewsByUser(String pathReviewsFile) throws Exception {
        int maxThreads	= 250;
        long c=0;

        // creating the thread factors
        ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
        BoundStatement bstmt;
        
        JSONObject json;
        String rname;
        String summary;
        String reviewText;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(pathReviewsFile));
            String line = reader.readLine();
            while (line != null) {
                c++;
                final long x = c;
                json = new JSONObject(line);

                try {
                	rname = json.getString("reviewerName");
                }
                catch (Exception e){
                	rname = NOT_AVAILABLE_VALUE;
                }
                
                try {
                	summary = json.getString("summary");
                }
                catch (Exception e){
                	summary = NOT_AVAILABLE_VALUE;
                }
                
                try {
                	reviewText = json.getString("reviewText");
                }
                catch (Exception e){
                	reviewText = NOT_AVAILABLE_VALUE;
                }
                
                
                bstmt = pUserReviewInsert.bind()
                		.setString(0, json.getString("reviewerID"))
                        .setInstant(1, Instant.ofEpochSecond(json.getLong("unixReviewTime")))
                        .setString(2, rname)
                        .setString(3, json.getString("asin"))
                        .setFloat(4, (float)json.getDouble("overall"))
                        .setString(5, summary)
                        .setString(6, reviewText);
                final BoundStatement bound = bstmt;

                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        session.execute(bound);
                        System.out.println("ReviewsByUser - added " + x);
                    }
                });
                line = reader.readLine();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
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
