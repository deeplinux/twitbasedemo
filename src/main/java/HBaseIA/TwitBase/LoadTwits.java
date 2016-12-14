package HBaseIA.TwitBase;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.joda.time.DateTime;

import HBaseIA.TwitBase.hbase.TwitsDAO;
import HBaseIA.TwitBase.hbase.UsersDAO;
import HBaseIA.TwitBase.model.User;
import utils.LoadUtils;

public class LoadTwits {
	
	static {
		System.setProperty("hadoop.home.dir", "D:/MyTools/hadoop-common-2.2.0-bin-master");
	}
	
	private static DateTime randDT() {
		int year = 2010 + LoadUtils.randInt(5);
		int month = 1 + LoadUtils.randInt(12);
		int day = 1 + LoadUtils.randInt(28);
		return new DateTime(year, month, day, 0, 0, 0, 0);
	}
	
	private static String randTwit(List<String> words) {
		String twit = "";
		for (int i = 0; i < 12; i++) {
			twit += LoadUtils.randNth(words) + " ";
		}
		return twit;
	}

	public static void main(String[] args) throws IOException {
		Date first = new Date();
		Configuration conf = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(conf);
				Admin admin = connection.getAdmin()) {
			TableName userTableName = TableName.valueOf(UsersDAO.TABLE_NAME);
			TableName twitsTableName = TableName.valueOf(TwitsDAO.TABLE_NAME);
			if(!admin.tableExists(userTableName) ||
					!admin.tableExists(twitsTableName)) {
				 System.out.println("Please use the InitTables utility to create " +
                        "destination tables first.");
				 System.exit(0);
			}
			
			UsersDAO users = new UsersDAO(connection);
			TwitsDAO twits = new TwitsDAO(connection);
			
			int count = 5;
			List<String> words = LoadUtils.readResource(LoadUtils.WORDS_PATH);
			 
		    for(User u : users.getUsers()) {
		        for (int i = 0; i < count; i++) {
		        	String twit = randTwit(words);
		            twits.postTwit(u.user, randDT(), twit);
		            users.incTweetCount(u.user);
		            System.out.println("用户:"+u.user+"说:"+twit);
		        }
		    }
		    System.out.println("结束耗时:"+(new Date().getTime()-first.getTime())/1000+"秒");
		}
		
	}

}
