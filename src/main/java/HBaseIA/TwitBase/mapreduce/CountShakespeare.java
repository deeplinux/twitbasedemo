package HBaseIA.TwitBase.mapreduce;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import HBaseIA.TwitBase.hbase.TwitsDAO;


public class CountShakespeare {
	
	static {
		System.setProperty("hadoop.home.dir", "D:/MyTools/hadoop-common-2.2.0-bin-master");
	}
	
	public static class Map 
		extends TableMapper<Text,LongWritable> {
		
		public static enum Counters {ROWS,SHAKESPEAREAN};
		private Random rand;
		
		private boolean containsShakespear(String msg) {
		      return rand.nextBoolean();
		}
		
	    @Override
	    protected void setup(Context context) {
	      rand = new Random(System.currentTimeMillis());
	    }
		
	    @Override
	    protected void map(ImmutableBytesWritable rowkey,
	            Result result,
	            Context context) {
	    	byte[] b = CellUtil.cloneValue(result.getColumnLatestCell(
	    			TwitsDAO.TWITS_FAM, 
	    			TwitsDAO.TWIT_COL));
	    	if(b==null) return;
	    	
	    	String msg = Bytes.toHex(b);
	    	if(msg.isEmpty()) {
	    		return;
	    	}
	    	
	    	context.getCounter(Counters.ROWS).increment(1);
	    	if(containsShakespear(msg)) {
	    		context.getCounter(Counters.SHAKESPEAREAN).increment(1);
	    	}
	    }
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf,"TwitBase Shakespeare counter");
		job.setJarByClass(CountShakespeare.class);
		
		Scan scan = new Scan();
		scan.addColumn(TwitsDAO.TWITS_FAM, TwitsDAO.TWIT_COL);
	    TableMapReduceUtil.initTableMapperJob(
	    	      Bytes.toString(TwitsDAO.TABLE_NAME),
	    	      scan,
	    	      Map.class,
	    	      ImmutableBytesWritable.class,
	    	      Result.class,
	    	      job);
		
	    job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true)?0:1);
	    
	}
}
