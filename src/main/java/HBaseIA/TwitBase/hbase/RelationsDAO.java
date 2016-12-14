package HBaseIA.TwitBase.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


import utils.Md5Utils;

public class RelationsDAO {
	public static final byte[] FOLLOWS_TABLE_NAME = Bytes.toBytes("follows");
	public static final byte[] FOLLOWED_TABLE_NAME = Bytes.toBytes("followedBy");
	public static final byte[] RELATION_FAM = Bytes.toBytes("f");
	public static final byte[] FROM = Bytes.toBytes("from");
	public static final byte[] TO = Bytes.toBytes("to");

	private static final int KEY_WIDTH = 2 * Md5Utils.MD5_LENGTH;

	private Connection connection;

	public RelationsDAO(Connection connection) {
		this.connection = connection;
	}

	public void doFollows(String follower, String followed) throws IOException {
		addRelation(FOLLOWS_TABLE_NAME, follower, followed);
	}
	
	public void addFollows(String follower, String followed) throws IOException {
		addRelation(FOLLOWS_TABLE_NAME, follower, followed);
	}
	
	
	public void addFollowedBy(String followed, String follower) throws IOException {
		addRelation(FOLLOWED_TABLE_NAME, followed, follower);
	}

	public void addRelation(byte[] table, String from, String to) throws IOException {

		Table t1 = connection.getTable(TableName.valueOf(table));
		
		Put p = new Put(mkRowKey(from, to));
		p.addColumn(RELATION_FAM, FROM, Bytes.toBytes(from));
		p.addColumn(RELATION_FAM, TO, Bytes.toBytes(to));
		t1.put(p);

		t1.close();
	}
	
	public static byte[] mkRowKey(String a, String b) {
		byte[] ahash = Md5Utils.md5sum(a);
		byte[] bhash = Md5Utils.md5sum(b);
		byte[] rowkey = new byte[KEY_WIDTH];

		int offset = 0;
		offset = Bytes.putBytes(rowkey, offset, ahash, 0, ahash.length);
		Bytes.putBytes(rowkey, offset, bhash, 0, bhash.length);
		return rowkey;
	}
	
	public List<HBaseIA.TwitBase.model.Relation> listFollows(String fromId) throws IOException {
		return listRelations(FOLLOWS_TABLE_NAME, fromId);
	}

	public List<HBaseIA.TwitBase.model.Relation> listFollowedBy(String fromId) throws IOException {
		return listRelations(FOLLOWED_TABLE_NAME, fromId);
	}

	public List<HBaseIA.TwitBase.model.Relation> listRelations(byte[] table, String fromId) throws IOException {
		Table t = connection.getTable(TableName.valueOf(table));
		String rel = (Bytes.equals(table, FOLLOWS_TABLE_NAME)) ? "->" : "<-";

		byte[] startKey = mkRowKey(fromId);
		byte[] endKey = Arrays.copyOf(startKey, startKey.length);
		endKey[Md5Utils.MD5_LENGTH - 1]++;
		Scan scan = new Scan(startKey, endKey);
		scan.addColumn(RELATION_FAM, TO);
		scan.setMaxVersions(1);

		ResultScanner results = t.getScanner(scan);
		List<HBaseIA.TwitBase.model.Relation> ret = new ArrayList<HBaseIA.TwitBase.model.Relation>();
		for (Result r : results) {
			Cell cell = r.getColumnLatestCell(RELATION_FAM, TO);
			String toId = Bytes.toString(CellUtil.cloneValue(cell));
			ret.add(new Relation(rel, fromId, toId));
		}

		t.close();
		return ret;
	}
	
	public static byte[] mkRowKey(String a) {
		byte[] ahash = Md5Utils.md5sum(a);
		byte[] rowkey = new byte[KEY_WIDTH];

		Bytes.putBytes(rowkey, 0, ahash, 0, ahash.length);
		return rowkey;
	}
	  
	private static class Relation extends HBaseIA.TwitBase.model.Relation {

		private Relation(String relation, String from, String to) {
			this.relation = relation;
			this.from = from;
			this.to = to;
		}
	}
}
