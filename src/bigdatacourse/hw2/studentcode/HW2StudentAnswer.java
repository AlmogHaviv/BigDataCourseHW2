package bigdatacourse.hw2.studentcode;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
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

/**
 * Implementation of HW2API for handling Amazon product data using Apache Cassandra.
 * This class manages items and reviews data through three main tables:
 * - items: Stores product information
 * - user_reviews: Stores reviews organized by reviewer
 * - item_reviews: Stores reviews organized by item
 */
public class HW2StudentAnswer implements HW2API {
    
    // Constants for general use
    public static final String NOT_AVAILABLE_VALUE = "na";

    // Table names for Cassandra schema
    private static final String TABLE_BY_ITEM = "items";
    private static final String TABLE_BY_REVIEWR = "user_reviews";
    private static final String TABLE_BY_ITEMID = "item_reviews";
    
    // CQL statements for table creation
    private static final String CQL_CREATE_TABLE_FOR_ITEMS = 
            "CREATE TABLE " + TABLE_BY_ITEM + "(" + 
                "asin text," +
                "title text," +
                "image text," +
                "categories set<text>," +
                "description text," +
                "PRIMARY KEY (asin)" +
            ")";
    
    // Table for storing reviews by reviewer with clustering order
    private static final String CQL_CREATE_TABLE_FOR_REVIWERS = 
            "CREATE TABLE " + TABLE_BY_REVIEWR + "(" + 
                "reviewerID text," +
                "unixReviewTime bigint," +
                "asin text," +
                "reviewerName text," +
                "overall float," +
                "description text," +
                "summary text," +
                "PRIMARY KEY (reviewerID, unixReviewTime, asin)" +
            ") " +
            "WITH CLUSTERING ORDER BY (unixReviewTime DESC, asin ASC)";
    
    // Table for storing reviews by item with clustering order
    private static final String CQL_CREATE_TABLE_FOR_ITEMS_REVIEWS = 
            "CREATE TABLE " + TABLE_BY_ITEMID + "(" + 
                "asin text," +
                "unixReviewTime bigint," +
                "reviewerID text," +
                "reviewerName text," +
                "overall float," +
                "description text," +
                "summary text," +
                "PRIMARY KEY (asin, unixReviewTime, reviewerID)" +
            ") " +
            "WITH CLUSTERING ORDER BY (unixReviewTime DESC, reviewerID ASC)";
    
    // Cassandra session for database operations
    private CqlSession session;
    
    // CQL statements for data manipulation
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
    
    // Prepared statements for optimized query execution
    private PreparedStatement pstmtSelect;
    private PreparedStatement pstmtInsertItem;
    private PreparedStatement pstmtInsertReviewer;
    private PreparedStatement pstmtInsertItemReview;
    private PreparedStatement pstmtQueryByReviewer;
    private PreparedStatement pstmtQueryByItem;
    
    /**
     * Establishes connection to Cassandra database using provided credentials
     */
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

    /**
     * Closes the Cassandra session
     */
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

    /**
     * Creates the required tables in Cassandra
     */
    @Override
    public void createTables() {
        session.execute(CQL_CREATE_TABLE_FOR_ITEMS);
        System.out.println("created table: " + TABLE_BY_ITEM);
        session.execute(CQL_CREATE_TABLE_FOR_REVIWERS);
        System.out.println("created table: " + TABLE_BY_REVIEWR);
        session.execute(CQL_CREATE_TABLE_FOR_ITEMS_REVIEWS);
        System.out.println("created table: " + TABLE_BY_ITEMID);
    }

    /**
     * Initializes prepared statements for database operations
     */
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

    /**
     * Loads items from JSON file into Cassandra using multi-threaded approach
     * Uses a thread pool to handle concurrent insertions
     */
    @Override
    public void loadItems(String pathItemsFile) throws Exception {
        int maxThreads = 240; // Number of threads for parallel processing
        ExecutorService executor = Executors.newFixedThreadPool(maxThreads);

        int totalLines = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(new File(pathItemsFile)))) {
            String line;

            while ((line = reader.readLine()) != null) {
                final String jsonLine = line;
                executor.execute(() -> {
                    try {
                        // Parse JSON and extract item data
                        JSONObject item = new JSONObject(jsonLine);
                        String asin = item.getString("asin");
                        String title = item.optString("title", null);
                        String image = item.optString("imUrl", null);
                        String description = item.optString("description", null);

                        // Process categories from nested JSON array
                        Set<String> categories = new HashSet<>();
                        JSONArray categoriesArray = item.getJSONArray("categories");
                        for (int j = 0; j < categoriesArray.length(); j++) {
                            JSONArray categoryList = categoriesArray.getJSONArray(j);
                            for (int k = 0; k < categoryList.length(); k++) {
                                categories.add(categoryList.getString(k));
                            }
                        }

                        // Build and execute insert statement
                        BoundStatement bstmt = pstmtInsertItem.bind(asin);
                        if (title != null && !title.isEmpty()) bstmt = bstmt.setString("title", title);
                        if (image != null && !image.isEmpty()) bstmt = bstmt.setString("image", image);
                        if (description != null && !description.isEmpty()) bstmt = bstmt.setString("description", description);
                        if (categories != null && !categories.isEmpty()) {
                            bstmt = bstmt.setSet("categories", categories, String.class);
                        }

                        CompletableFuture<AsyncResultSet> future = session.executeAsync(bstmt).toCompletableFuture();
                        future.join();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                totalLines++;
            }
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        System.out.println("Total lines inserted: " + totalLines);
    }

    /**
     * Loads reviews from JSON file into Cassandra using multi-threaded approach
     * Inserts each review into both reviewer-based and item-based tables
     */
    @Override
    public void loadReviews(String pathReviewsFile) throws Exception {
        int maxThreads = 240;
        ExecutorService executor = Executors.newFixedThreadPool(maxThreads);

        int totalLines = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(new File(pathReviewsFile)))) {
            String line;

            while ((line = reader.readLine()) != null) {
                final String jsonLine = line;
                executor.execute(() -> {
                    try {
                        // Parse JSON and extract review data
                        JSONObject review = new JSONObject(jsonLine);
                        String reviewerID = review.getString("reviewerID");
                        String asin = review.getString("asin");
                        String reviewerName = review.optString("reviewerName", null);
                        float overall = (float) review.optDouble("overall", -1);
                        String description = review.optString("reviewText", null);
                        String summary = review.optString("summary", null);
                        long unixReviewTime = review.getLong("unixReviewTime");

                        // Insert into reviewer-based table
                        BoundStatement bstmtReviewer = pstmtInsertReviewer.bind(reviewerID, unixReviewTime, asin);
                        if (reviewerName != null && !reviewerName.isEmpty()) bstmtReviewer = bstmtReviewer.setString("reviewerName", reviewerName);
                        if (overall != 0) bstmtReviewer = bstmtReviewer.setFloat("overall", overall);
                        if (description != null && !description.isEmpty()) bstmtReviewer = bstmtReviewer.setString("description", description);
                        if (summary != null && !summary.isEmpty()) bstmtReviewer = bstmtReviewer.setString("summary", summary);

                        // Insert into item-based table
                        BoundStatement bstmtItemReview = pstmtInsertItemReview.bind(asin, unixReviewTime, reviewerID);
                        if (reviewerName != null && !reviewerName.isEmpty()) bstmtItemReview = bstmtItemReview.setString("reviewerName", reviewerName);
                        if (overall != -1) bstmtItemReview = bstmtItemReview.setFloat("overall", overall);
                        if (description != null && !description.isEmpty()) bstmtItemReview = bstmtItemReview.setString("description", description);
                        if (summary != null && !summary.isEmpty()) bstmtItemReview = bstmtItemReview.setString("summary", summary);

                        // Execute both inserts asynchronously
                        CompletableFuture<AsyncResultSet> future1 = session.executeAsync(bstmtReviewer).toCompletableFuture();
                        CompletableFuture<AsyncResultSet> future2 = session.executeAsync(bstmtItemReview).toCompletableFuture();

                        CompletableFuture.allOf(future1, future2).join();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                totalLines++;
            }
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        System.out.println("Total lines inserted: " + totalLines);
    }

    /**
     * Retrieves item information by ASIN
     * @param asin The Amazon Standard Identification Number
     * @return Formatted item information string or "not exists" if not found
     */
    @Override
    public String item(String asin) {
        // Bind the ASIN to the prepared statement
        BoundStatement bstmt = pstmtSelect.bind(asin);
        ResultSet rs = session.execute(bstmt);
        Row row = rs.one();
        
        if (row != null) {
            // Extract the categories set and item details
            Set<String> categories = row.getSet("categories", String.class);
            String item = formatItem(
                    row.getString("asin"),
                    row.getString("title"),
                    row.getString("image"),
                    categories,
                    row.getString("description")
            );
            return item;
        } else {
            return "not exists" + "\n";
        }
    }

    /**
     * Retrieves all reviews written by a specific reviewer
     * Results are ordered by review time (descending) and ASIN
     * @param reviewerID The unique identifier of the reviewer
     * @return Iterable collection of formatted review strings
     */
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
            String reviewRepr = formatReview(reviewTime, asin, reviewerID, reviewerName, 
                                           overall, summary, description);
            
            // Add the formatted review to the list
            reviewRepers.add(reviewRepr);
        }

        // Print the total number of reviews for debugging purposes
        System.out.println("total reviews: " + reviewRepers.size());
        
        // Return the list of formatted reviews
        return reviewRepers;
    }

    /**
     * Retrieves all reviews for a specific item (product)
     * Results are ordered by review time (descending) and reviewer ID
     * @param asin The Amazon Standard Identification Number of the item
     * @return Iterable collection of formatted review strings
     */
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
            String reviewRepr = formatReview(reviewTime, asin, reviewerID, reviewerName, 
                                           overall, summary, description);
            
            // Add the formatted review to the list
            reviewRepers.add(reviewRepr);
        }

        // Print the total number of reviews
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
