package HBaseIA.TwitBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import HBaseIA.TwitBase.hbase.RelationsDAO;
import HBaseIA.TwitBase.hbase.UsersDAO;
import HBaseIA.TwitBase.model.User;
import utils.LoadUtils;

public class loadRelations {
	private static final Logger log = Logger.getLogger(loadRelations.class);
	
	static {
		System.setProperty("hadoop.home.dir", "D:/MyTools/hadoop-common-2.2.0-bin-master");
	}
	
	public static void main(String[] args) throws IOException {
		Date first = new Date();
		Configuration conf = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(conf)) {
			RelationsDAO relations = new RelationsDAO(connection);
			List<User> users = new UsersDAO(connection).getUsers();
			int count = 5;
			List<String> userStrs = new ArrayList<String>(users.size()); 
			for(User user : users) {
				userStrs.add(user.name);
			}
			for(String follower :userStrs) {
				for(;count<5;count++) {
					String followed = randUser(userStrs);
					if(!follower.equals(followed)) {
						relations.addFollows(follower,followed);
						System.out.println(String.format("Adding follower %s -> %s", follower, followed));
					}	
				}
			}
		}
		System.out.println("结束耗时:"+(new Date().getTime()-first.getTime())/1000+"秒");
	}

	private static String randUser(List<String> users) {
		String user = LoadUtils.randNth(users);
 		return user;
	}
}
