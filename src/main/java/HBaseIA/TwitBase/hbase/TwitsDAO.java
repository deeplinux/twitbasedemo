package HBaseIA.TwitBase.hbase;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import HBaseIA.TwitBase.model.Twit;
import utils.Md5Utils;

public class TwitsDAO {
	public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
	public static final byte[] TWITS_FAM = Bytes.toBytes("twits");

	public static final byte[] USER_COL = Bytes.toBytes("user");
	public static final byte[] TWIT_COL = Bytes.toBytes("twit");
	private static final int longLength = 8; // bytes

	private static final Logger log = Logger.getLogger(TwitsDAO.class);

	private Connection connection;

	public TwitsDAO(Connection connection) {
		this.connection = connection;
	}

	private static byte[] mkRowKey(Twit t) {
		return mkRowKey(t.user, t.dt);
	}
	  
	private static byte[] mkRowKey(String user, DateTime dt) {
		byte[] userHash = Md5Utils.md5sum(user);
		byte[] timestamp = Bytes.toBytes(-1 * dt.getMillis());
		byte[] rowKey = new byte[Md5Utils.MD5_LENGTH + longLength];

		int offset = 0;
		offset = Bytes.putBytes(rowKey, offset, userHash, 0, userHash.length);
		Bytes.putBytes(rowKey, offset, timestamp, 0, timestamp.length);
		return rowKey;
	}
	  
	private static Put mkPut(Twit t) {
		Put p = new Put(mkRowKey(t));
		p.addColumn(TWITS_FAM, USER_COL, Bytes.toBytes(t.user));
		p.addColumn(TWITS_FAM, TWIT_COL, Bytes.toBytes(t.text));
		return p;
	}

	public void postTwit(String user, DateTime dt, String text) throws IOException {

		Table twits = connection.getTable(TableName.valueOf(TABLE_NAME));

		Put p = mkPut(new Twit(user, dt, text));
		twits.put(p);

		twits.close();
	}

	private static class Twit extends HBaseIA.TwitBase.model.Twit {

		private Twit(Result r) {
			this(CellUtil.cloneValue(r.getColumnLatestCell(TWITS_FAM, USER_COL)),
					Arrays.copyOfRange(r.getRow(), Md5Utils.MD5_LENGTH, Md5Utils.MD5_LENGTH + longLength),
					CellUtil.cloneValue(r.getColumnLatestCell(TWITS_FAM, TWIT_COL)));
		}

		private Twit(byte[] user, byte[] dt, byte[] text) {
			this(Bytes.toString(user), new DateTime(-1 * Bytes.toLong(dt)), Bytes.toString(text));
		}

		private Twit(String user, DateTime dt, String text) {
			this.user = user;
			this.dt = dt;
			this.text = text;
		}
	}

}
