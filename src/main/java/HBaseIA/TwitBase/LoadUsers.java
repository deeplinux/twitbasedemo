package HBaseIA.TwitBase;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import HBaseIA.TwitBase.hbase.UsersDAO;
import utils.LoadUtils;

public class LoadUsers {

	static {
		System.setProperty("hadoop.home.dir", "D:/MyTools/hadoop-common-2.2.0-bin-master");
	}
	
	private static String randName(List<String> names) {
		    String name = LoadUtils.randNth(names) + " ";
		    name += LoadUtils.randNth(names);
		    return name;
	}
	
    private static String randUser(String name) {
		    return String.format("%s%2d", name.substring(5), LoadUtils.randInt(100));
	}

	private static String randEmail(String user, List<String> words) {
		    return String.format("%s@%s.com", user, LoadUtils.randNth(words));
	}
	
	public static void main(String[] args) throws Exception {
		Date first = new Date();
		int count = 10000;
		Configuration config = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(config);) {
			UsersDAO dao = new UsersDAO(connection);
		    List<String> names = LoadUtils.readResource(LoadUtils.NAMES_PATH);
		    List<String> words = LoadUtils.readResource(LoadUtils.WORDS_PATH);
		    for (int i = 0; i < count; i++) {
		        String name = randName(names);
		        String user = randUser(name);
		        String email = randEmail(user, words);
		        dao.addUser(user, name, email, "abc123");
		    }	
		}
		System.out.println("结束耗时:"+(new Date().getTime()-first.getTime())/1000+"秒");
	}
	
}
