/**
 * 
 */
package spcache;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * @author sulf
 *
 */
public class Loader {

	public static void main(String[] args) throws Exception {
		DatabaseConn sqlConn = new DatabaseConn();

		Connection conn = sqlConn.getConnection();
		PreparedStatement stat = conn
				.prepareStatement("select TOP 10 * from "
						+ MongoDB.FLT_SHARE_SPECIAL_POLICY_OW + " WITH(NOLOCK) "
						+ " WHERE  PolicyStatus=0 AND (InventoryType = 1 OR (InventoryType = 2 AND TicketInventory > SaledTicketNum))"
						);
		ResultSet rs = stat.executeQuery();
		while (rs.next()) {
			System.out.println( rs.getInt("ProductID") );
			System.out.println( rs.getString("Dcity") );
			System.out.println( rs.getString("Acity") );
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
			keys.put(colFieldMapping.get("ProductID"), 1);
			keys.put(colFieldMapping.get("Dcity"), 1);
			keys.put(colFieldMapping.get("Acity"), 1);
			coll.createIndex(keys);
		} catch (Exception e) {
			logger.error("create mongo index failed", e);
			;
		}
	}
	
	public String getMongoField(String sqlCol) {
		return colFieldMapping.get(sqlCol);
	}
	
	// 
	private static Map<String, String> colFieldMapping = new HashMap<String, String>();
	static {
		try {
			List<String> files = Files.readAllLines(Paths.get(MongoDB.class.getResource("/fields.txt").getPath()), Charset.defaultCharset());
			int i = 0;
			for (String s : files) {
				if (!s.isEmpty()) {
					colFieldMapping.put(s, "f" + (i++));
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
