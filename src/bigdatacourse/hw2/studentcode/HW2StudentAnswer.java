package bigdatacourse.hw2.studentcode;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

import bigdatacourse.hw2.HW2API;

public class HW2StudentAnswer implements HW2API{
	
	// general consts
	public static final String		NOT_AVAILABLE_VALUE 	=		"na";

	// CQL stuff
	private static final String		TABLE_BY_ITEM = "items";
	private static final String		TABLE_BY_REVIEWR = "user_reviews";
	private static final String		TABLE_BY_ITEMID = "item_reviews";
		
	private static final String		CQL_CREATE_TABLE_FOR_ITEMS = 
			"CREATE TABLE " + TABLE_BY_ITEM 	+"(" 		+ 
				"asin text,"			                +
				"title text,"				            +
				"image text,"			                +
				"categories  set<text>,"		    +
				" description text,"				+
				"PRIMARY KEY (asin)"	        +
			") ";
	
	private static final String CQL_CREATE_TABLE_FOR_REVIWERS = 
			"CREATE TABLE " + TABLE_BY_REVIEWR 	+  "(" 		+ 
				"reviewerID text,"			                +
				"unixReviewTime bigint,"				            +
				"asin text,"			                +
				"reviewerName text,"                                                                +
				"overall float,"                                                                           +
				"description text,"				                                                        +
				"summary text,"                                                                       +
				"PRIMARY KEY (reviewerID, unixReviewTime, asin)"	    +
			") "+
			"WITH CLUSTERING ORDER BY (unixReviewTime DESC, asin ASC)";
	
	private static final String CQL_CREATE_TABLE_FOR_ITEMS_REVIEWS = 
			"CREATE TABLE " + TABLE_BY_ITEMID 	+"(" 		+ 
					"asin text," +
					"unixReviewTime bigint,"				            +
					"reviewerID text,"			                +
				"reviewerName text," +
				"overall float,"                 +
				"description text,"				+
				"summary text,"                                                                       +
				"PRIMARY KEY (asin, unixReviewTime, reviewerID)"	+
			") " +
			"WITH CLUSTERING ORDER BY (unixReviewTime DESC, reviewerID ASC)";
	
	
	// cassandra session
	private CqlSession session;
	
	// prepared statements
	 private static final String CQL_ITEM_INSERT = 
	            "INSERT INTO " + TABLE_BY_ITEM + "(asin, title, image, categories, description) VALUES(?, ?, ?, ?, ?)";
	 
	 private static final String CQL_INSERT_REVIEWER = 
			    "INSERT INTO " + TABLE_BY_REVIEWR + "(reviewerID, unixReviewTime, asin, reviewerName, overall, description, summary) " + 
			    "VALUES (?, ?, ?, ?, ?, ?, ?)";

	 private static final String CQL_INSERT_ITEM_REVIEW = 
			    "INSERT INTO " + TABLE_BY_ITEMID + "(asin, unixReviewTime, reviewerID, reviewerName, overall, description, summary) " +
			    "VALUES (?, ?, ?, ?, ?, ?, ?)";
	 
	 private static final String CQL_ITEM_SELECT = 
				"SELECT * FROM " + TABLE_BY_ITEM + " WHERE asin = ?";
	 
	 private static final String CQL_SELECT_REVIEWS_BY_REVIEWER =
			 "SELECT * FROM " + TABLE_BY_REVIEWR + " WHERE reviewerID = ?";
	 
	 private static final String CQL_SELECT_REVIEWS_BY_ITEM = 
			    "SELECT * FROM " + TABLE_BY_ITEMID + " WHERE asin = ?";
	
	
	private PreparedStatement pstmtSelect; 
	private PreparedStatement pstmtInsertItem;
	private PreparedStatement pstmtInsertReviewer;
	private PreparedStatement pstmtInsertItemReview;
	private PreparedStatement pstmtQueryByReviewer;
	private PreparedStatement pstmtQueryByItem;

	
	
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
		session.execute(CQL_CREATE_TABLE_FOR_ITEMS);
		System.out.println("created table: " + TABLE_BY_ITEM);
		session.execute(CQL_CREATE_TABLE_FOR_REVIWERS);
		System.out.println("created table: " + TABLE_BY_REVIEWR);
		session.execute(CQL_CREATE_TABLE_FOR_ITEMS_REVIEWS);
		System.out.println("created table: " + TABLE_BY_ITEMID);
	}

	@Override
	public void initialize() {
		this.pstmtSelect = session.prepare(CQL_ITEM_SELECT);
		this.pstmtInsertItem = session.prepare(CQL_ITEM_INSERT);
		this.pstmtInsertReviewer = session.prepare(CQL_INSERT_REVIEWER);
		this.pstmtInsertItemReview = session.prepare(CQL_INSERT_ITEM_REVIEW);
		this.pstmtQueryByReviewer = session.prepare(CQL_SELECT_REVIEWS_BY_REVIEWER);
		this.pstmtQueryByItem = session.prepare(CQL_SELECT_REVIEWS_BY_ITEM);
		System.out.println("All function are initialized");
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
	
		// Read the JSON file
        BufferedReader reader = new BufferedReader(new FileReader(new File(pathItemsFile)));
        String line;

        while ((line = reader.readLine()) != null) {
            // Parse each line as a JSONObject
            JSONObject item = new JSONObject(line);

            // Extract fields from JSON
            String asin = item.getString("asin");
            String title = item.optString("title", null); // title might be missing in some entries
            String image = item.optString("imUrl", null);
            String description = item.optString("description", null); 
            
            // Extract categories and convert them into a Set
            Set<String> categories = new HashSet<>();
            JSONArray categoriesArray = item.getJSONArray("categories");
            for (int j = 0; j < categoriesArray.length(); j++) {
                JSONArray categoryList = categoriesArray.getJSONArray(j);
                for (int k = 0; k < categoryList.length(); k++) {
                    categories.add(categoryList.getString(k));
                }
            }
            
            // Prepare the bind values for the insert statement
            BoundStatement bstmt = pstmtInsertItem.bind(asin, title, image, categories, description);
            session.execute(bstmt);
        }
        
        System.out.println("Inserted");
	}

	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		// Read the JSON file
	    BufferedReader reader = new BufferedReader(new FileReader(new File(pathReviewsFile)));
	    String line;

	    while ((line = reader.readLine()) != null) {
	        // Parse each line as a JSONObject
	        JSONObject review = new JSONObject(line);

	        // Extract fields from JSON
	        String reviewerID = review.getString("reviewerID");
	        String asin = review.getString("asin");
	        String reviewerName = review.optString("reviewerName", null);
	        float overall = (float) review.optDouble("overall", 0);
	        String description = review.optString("reviewText", null);
	        String summary = review.optString("summary", null);
	        long unixReviewTime = review.getLong("unixReviewTime");

	        // Prepare the bind values for the insert statement for the first table: TABLE_BY_REVIEWER
	        BoundStatement bstmtReviewer = pstmtInsertReviewer.bind(reviewerID, unixReviewTime, asin, reviewerName, overall, description, summary);
	        session.execute(bstmtReviewer);

	        // Prepare the bind values for the insert statement for the second table: TABLE_BY_ITEMID
	        BoundStatement bstmtItemReview = pstmtInsertItemReview.bind(asin, unixReviewTime, reviewerID, reviewerName, overall, description, summary);
	        session.execute(bstmtItemReview);
	    }

	    System.out.println("Inserted reviews into the tables");
	}

	@Override
	public String item(String asin) {
		// Bind the asin parameter to the prepared statement
	    BoundStatement bstmt = pstmtSelect.bind(asin);
	    
	    // Execute the prepared statement
	    ResultSet rs = session.execute(bstmt);
	    
	    // Initialize the count variable to iterate over the rows
	    Row row = rs.one();  // Get the first row
	    
	    if (row != null) {
	        // Retrieve the categories as a Set<String>
	        Set<String> categories = row.getSet("categories", String.class);  // Use getSet() to get the Set<String>
	        
	        // Format the item using the retrieved data
	        String item = formatItem(
	                row.getString("asin"), 
	                row.getString("title"),
	                row.getString("image"), 
	                categories,  // Directly use the Set<String> for categories
	                row.getString("description")
	        );
	        
	        return item;
	    
	    } else {
	        return "not exists";
	    }
	}
	
	
	@Override
	public Iterable<String> userReviews(String reviewerID) {
		 // Initialize a list to store the formatted reviews
	    ArrayList<String> reviewRepers = new ArrayList<>();
	    
	    // Prepare the statement with the reviewerID
	    BoundStatement bstmt = pstmtQueryByReviewer.bind(reviewerID);
	    
	    // Execute the query and retrieve the result set
	    ResultSet rs = session.execute(bstmt);
	    
	    // Loop through the result set and process each review
	    for (Row row : rs) {
	        // Convert the unixReviewTime to Instant and extract the other fields
	        Instant reviewTime = Instant.ofEpochSecond(row.getLong("unixReviewTime"));
	        String asin = row.getString("asin");
	        String reviewerName = row.getString("reviewerName");
	        int overall = (int) row.getFloat("overall");
	        String description = row.getString("description");
	        String summary = row.getString("summary");
	        
	        // Format the review using the extracted data
	        String reviewRepr = formatReview(reviewTime, asin, reviewerID, reviewerName, overall, summary, description);
	        
	        // Add the formatted review to the list
	        reviewRepers.add(reviewRepr);
	    }

	    // Print the total number of reviews for debugging purposes
	    System.out.println("total reviews: " + reviewRepers.size());
	    
	    // Return the list of formatted reviews
	    return reviewRepers;
	}

	@Override
	public Iterable<String> itemReviews(String asin) {
		// Initialize a list to store the formatted reviews
	    ArrayList<String> reviewRepers = new ArrayList<>();
	    
	    // Prepare the statement with the asin parameter
	    BoundStatement bstmt = pstmtQueryByItem.bind(asin);
	    
	    // Execute the query and retrieve the result set
	    ResultSet rs = session.execute(bstmt);
	    
	    // Loop through the result set and process each review
	    for (Row row : rs) {
	        // Convert the unixReviewTime to Instant and extract the other fields
	        Instant reviewTime = Instant.ofEpochSecond(row.getLong("unixReviewTime"));
	        String reviewerID = row.getString("reviewerID");
	        String reviewerName = row.getString("reviewerName");
	        int overall = (int) row.getFloat("overall");
	        String description = row.getString("description");
	        String summary = row.getString("summary");
	        
	        // Format the review using the extracted data
	        String reviewRepr = formatReview(reviewTime, asin, reviewerID, reviewerName, overall, summary, description);
	        
	        // Add the formatted review to the list
	        reviewRepers.add(reviewRepr);
	    }

	    // Print the total number of reviews for debugging purposes
	    System.out.println("total reviews: " + reviewRepers.size());
	    
	    // Return the list of formatted reviews
	    return reviewRepers;
	}
	
	
	// Formatting methods, do not change!
	private String formatItem(String asin, String title, String imageUrl, Set<String> categories, String description) {
		String itemDesc = "";
		itemDesc += "asin: " + asin + "\n";
		itemDesc += "title: " + title + "\n";
		itemDesc += "image: " + imageUrl + "\n";
		itemDesc += "categories: " + categories.toString() + "\n";
		itemDesc += "description: " + description + "\n";
		return itemDesc;
	}

	private String formatReview(Instant time, String asin, String reviewerId, String reviewerName, Integer rating, String summary, String reviewText) {
		String reviewDesc = 
			"time: " + time + 
			", asin: " 	+ asin 	+
			", reviewerID: " 	+ reviewerId +
			", reviewerName: " 	+ reviewerName 	+
			", rating: " 		+ rating	+ 
			", summary: " 		+ summary +
			", reviewText: " 	+ reviewText + "\n";
		return reviewDesc;
	}

}
