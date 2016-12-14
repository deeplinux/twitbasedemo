package HBaseIA.TwitBase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import HBaseIA.TwitBase.model.User;

public class UsersDAO {
	public static final byte[] TABLE_NAME = Bytes.toBytes("users");
	public static final byte[] INFO_FAM = Bytes.toBytes("info");

	public static final byte[] USER_COL = Bytes.toBytes("user");
	public static final byte[] NAME_COL = Bytes.toBytes("name");
	public static final byte[] EMAIL_COL = Bytes.toBytes("email");
	public static final byte[] PASS_COL = Bytes.toBytes("password");
	public static final byte[] TWEETS_COL = Bytes.toBytes("tweet_count");

	public static final byte[] HAMLET_COL = Bytes.toBytes("hamlet_tag");

	private static final Logger log = Logger.getLogger(UsersDAO.class);

	private Connection connection;

	public UsersDAO(Connection connection) {
		this.connection = connection;
	}

	private static Put mkPut(User u) {
		log.debug(String.format("Creating Put for %s", u));

		Put p = new Put(Bytes.toBytes(u.user));
		p.addColumn(INFO_FAM, USER_COL, Bytes.toBytes(u.user));
		p.addColumn(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
		p.addColumn(INFO_FAM, EMAIL_COL, Bytes.toBytes(u.email));
		p.addColumn(INFO_FAM, PASS_COL, Bytes.toBytes(u.password));
		return p;
	}

	public static Put mkPut(String username, byte[] fam, byte[] qual, byte[] val) {
		Put p = new Put(Bytes.toBytes(username));
		p.addColumn(fam, qual, val);
		return p;
	}

	private static Scan mkScan() {
		Scan s = new Scan();
		s.addFamily(INFO_FAM);
		return s;
	}
	  
	public void addUser(String user, String name, String email, String password) throws IOException {

		Table users = connection.getTable(TableName.valueOf(TABLE_NAME));

		Put p = mkPut(new User(user, name, email, password));
		users.put(p);

		users.close();
	}

	public List<HBaseIA.TwitBase.model.User> getUsers() throws IOException {
		Table users = connection.getTable(TableName.valueOf(TABLE_NAME));
		ResultScanner results = users.getScanner(mkScan());
		ArrayList<HBaseIA.TwitBase.model.User> ret = new ArrayList<HBaseIA.TwitBase.model.User>();
		for (Result r : results) {
			ret.add(new User(r));
		}

		users.close();
		return ret;
	}

	private static class User extends HBaseIA.TwitBase.model.User {

		private User(Result r) {
			this(r.getValue(INFO_FAM, USER_COL), r.getValue(INFO_FAM, NAME_COL), r.getValue(INFO_FAM, EMAIL_COL),
					r.getValue(INFO_FAM, PASS_COL),
					r.getValue(INFO_FAM, TWEETS_COL) == null ? Bytes.toBytes(0L) : r.getValue(INFO_FAM, TWEETS_COL));
		}

		private User(byte[] user, byte[] name, byte[] email, byte[] password, byte[] tweetCount) {
			this(Bytes.toString(user), Bytes.toString(name), Bytes.toString(email), Bytes.toString(password));
			this.tweetCount = Bytes.toLong(tweetCount);
		}

		private User(String user, String name, String email, String password) {
			this.user = user;
			this.name = name;
			this.email = email;
			this.password = password;
		}
	}

	public long incTweetCount(String user) throws IllegalArgumentException, IOException {
		Table users = connection.getTable(TableName.valueOf(TABLE_NAME));
		long ret = users.incrementColumnValue(Bytes.toBytes(user), INFO_FAM, TWEETS_COL, 1L);

		users.close();
		return ret;
	}
}
