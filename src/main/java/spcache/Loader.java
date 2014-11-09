/**
 * 
 */
package spcache;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * @author sulf
 *
 */
public class Loader {

	private static final Logger logger = LoggerFactory.getLogger(Loader.class);

	public static void main(String[] args) throws Exception {
		loadMongoTest();

	}

	private static void loadMongoTest() throws Exception {
		MongoDB mongo = new MongoDB();
		DBCollection coll = mongo.getCollection();

		// load csv data
		List<String> files = Files.readLines(new File(MongoDB.class
				.getResource("/flight_top1000.csv").getPath()), Charset
				.forName("utf8"));
		String[] headers = files.get(0).split(",");
		
		final int totalSize = 70000000;
		Stopwatch watch = Stopwatch.createStarted();
		for (int j = 0; j < totalSize / 700; j++) {
		    List<DBObject> dbos = new ArrayList<DBObject>(files.size() - 1);
    		for (int i = 1; i < files.size(); i++) {
    			Optional<DBObject> op = mongo.convertTo(files.get(i).split(","),
    					headers);
    			if (op.isPresent()) {
    				dbos.add(op.get());
    			}
    		}
    		coll.insert(dbos);
    		
    		if ( (j % 100) == 0) {
    		    logger.info("inserted " + j * 771 + " lines");
    		}
		}
		
		watch.stop();
		logger.info(" all inserted using time :" + watch.elapsed(TimeUnit.MILLISECONDS) + " ms");
	}

	@SuppressWarnings("unused")
	private static void loadSqlTest() throws SQLException {
		DatabaseConn sqlConn = new DatabaseConn();

		Connection conn = sqlConn.getConnection();
		PreparedStatement stat = conn
				.prepareStatement("select TOP 10 * from "
						+ MongoDB.FLT_SHARE_SPECIAL_POLICY_OW
						+ " WITH(NOLOCK) "
						+ " WHERE  PolicyStatus=0 AND (InventoryType = 1 OR (InventoryType = 2 AND TicketInventory > SaledTicketNum))");
		ResultSet rs = stat.executeQuery();
		while (rs.next()) {
			System.out.println(rs.getInt("ProductID"));
			System.out.println(rs.getString("Dcity"));
			System.out.println(rs.getString("Acity"));
		}
	}

}

class MongoDB {
	public static final String FLT_TRADE_SPECIAL_DB = "FltTradeSpecialDB";
	public static final String FLT_SHARE_SPECIAL_POLICY_OW = "Flt_ShareSpecialPolicyOW";
	private static final Logger logger = LoggerFactory.getLogger(MongoDB.class);
	private MongoClient client;
	private DB db;
	private DBCollection coll;

	public MongoDB() throws Exception {
		client = new MongoClient("localhost:27017");
		db = client.getDB(FLT_TRADE_SPECIAL_DB);
		coll = db.getCollection(FLT_SHARE_SPECIAL_POLICY_OW);

		try {
			BasicDBObject keys = new BasicDBObject();
			keys.put(colFieldMapping.get("Dcity"), 1);
			keys.put(colFieldMapping.get("Acity"), 1);
			keys.put(colFieldMapping.get("ProductID"), 1);
			coll.createIndex(keys);
		} catch (Exception e) {
			logger.error("create mongo index failed", e);
		}
	}

	public DBCollection getCollection() {
		return coll;
	}

	private String getMongoField(String sqlCol) {
		return colFieldMapping.get(sqlCol);
	}

	public Optional<DBObject> convertTo(String[] row, String[] header) {
		if (row.length != header.length) {
			return Optional.absent();
		}
		BasicDBObject dbo = new BasicDBObject();
		for (int i = 0; i < row.length; i++) {
			String key = header[i];
			String mongoField = getMongoField(key);
			if (mongoField != null) {
				// add if defined
				if (row[i].toString().isEmpty()) {
					dbo.put(mongoField, row[i]);
				} else if (row[i].toString().toLowerCase().equals("null")) {
					dbo.put(mongoField, null);
				} else {
					try {
						dbo.put(mongoField, Integer.parseInt(row[i].toLowerCase()));
					} catch (Exception e) {
						dbo.put(mongoField, row[i]);
					}
				}
			}
		}
		return Optional.of((DBObject) dbo);
	}

	// TEST IF all ok to mongo, we might add other ASCII available character
	private static char[] AVAILABLE_FIELD_NAME = new char[] { 'a', 'b', 'c',
			'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
			'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C',
			'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
			'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2',
			'3', '4', '5', '6', '7', '8', '9', '`', '~', '!', '@', '#', '%',
			'^', '&', '*', '(', ')', '-', '_', '=', '+', '[', ']', '{', '}',
			':', ';', '"', '\'', '/', '?', '>', ',', '<', '|', '\\' };
	private static Map<String, String> colFieldMapping = new HashMap<String, String>();
	static {
		try {
			List<String> files = Files.readLines(new File(MongoDB.class
					.getResource("/fields.txt").getPath()), Charset
					.defaultCharset());
			int i = 0;
			for (String s : files) {
				if (!s.trim().isEmpty()) {
					colFieldMapping.put(s.replace(',', ' ').trim(),
							String.valueOf(AVAILABLE_FIELD_NAME[i++]));
				}
			}
			logger.info(colFieldMapping.toString());
		} catch (IOException e) {
			logger.info("failed", e);
		}
	}

}

class DatabaseConn {
	private static final Logger logger = LoggerFactory
			.getLogger(DatabaseConn.class);
	private Connection conn;
	/**
	 * // jrds driver
	 * 
	 * <pre>
	 * // String dbURL = &quot;jdbc:jtds:sqlserver://127.0.0.1:1433;;DatabaseName=test&quot;;
	 * </pre>
	 */
	private String url = "jdbc:sqlserver://dataentrydb.lpt.qa.nt.ctripcorp.com:55999;DatabaseName=FltTradeSpecialDB";
	private String classforname = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private String uid = "Uws_FltTradeSpecialDB_LptDataentryDB";
	private String pwd = "*7NI5Ni^2^!5";

	public DatabaseConn() {
	}

	public Connection getConnection() {
		try {
			Class.forName(classforname);
			if (conn == null || conn.isClosed()) {
				conn = DriverManager.getConnection(url, uid, pwd);
			}
		} catch (Exception ex) {
			logger.error("failed to create sql connection!", ex);
		}

		return conn;
	}

}
