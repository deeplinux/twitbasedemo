package HBaseIA.TwitBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import HBaseIA.TwitBase.hbase.RelationsDAO;
import HBaseIA.TwitBase.model.Relation;

public class RelationsTool {
	private static final Logger log = Logger.getLogger(RelationsTool.class);

	static {
		System.setProperty("hadoop.home.dir", "D:/MyTools/hadoop-common-2.2.0-bin-master");
	}

	public static void follows(RelationsDAO dao, String follower, String followed) throws IOException {
		log.debug(String.format("Adding follower %s -> %s", follower, followed));
		dao.addFollows(follower, followed);
		System.out.println("Successfully added relationship");
	}

	public static void list(RelationsDAO dao,String follower,String followed) throws IOException {
		List<Relation> followers = new ArrayList<Relation>();
		List<Relation> followeds = new ArrayList<Relation>();
		followeds.addAll(dao.listFollows(follower));
		followers.addAll(dao.listFollowedBy(followed));

		System.out.println(follower + "##############");
		if (followeds.isEmpty())
			System.out.println("No relations found.");
		for (Relation r : followeds) {
			System.out.println(r);
		}
		
		System.out.println(followed + "##############");
		if (followers.isEmpty())
			System.out.println("No relations found.");
		for (Relation r : followers) {
			System.out.println(r);
		}
		
	}

	public static void main(String[] args) throws IOException {
		String follower = "follower3";
		String followed = "followed3";

		Configuration conf = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(conf)) {
			RelationsDAO dao = new RelationsDAO(connection);
			follows(dao, follower, followed);
			list(dao, follower, followed);// 还没指定列
		}
	}

}
