package HBaseIA.TwitBase.coprocessors;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import com.google.protobuf.ServiceException;

import HBaseIA.TwitBase.coprocessors.Sum.SumRequest;
import HBaseIA.TwitBase.coprocessors.Sum.SumResponse;
import HBaseIA.TwitBase.coprocessors.Sum.SumService;

/*
 * 实现统计users表的人员个数
 * 0.96
 * HBase0.96之后使用的是protobuf2.5来作为服务端和客户端的rpc协议,在此使用HBASE官方例子中的代码，略微修改
 */
public class SumEndPointClient {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		// Use below code for HBase version 1.x.x or above.
		Connection connection = ConnectionFactory.createConnection(conf);
		TableName tableName = TableName.valueOf("users");
		Table table = connection.getTable(tableName);

		//Use below code HBase version 0.98.xx or below.
		//HConnection connection = HConnectionManager.createConnection(conf);
		//HTableInterface table = connection.getTable("users");

		final SumRequest request = SumRequest.newBuilder().setFamily("info").setColumn("user")
		                            .build();
		try {
		Map<byte[], Long> results = table.coprocessorService (SumService.class, null, null,
		new Batch.Call<SumService, Long>() {
		    	@Override
		        public Long call(SumService aggregate) throws IOException {
		    		BlockingRpcCallback rpcCallback = new BlockingRpcCallback();
		            aggregate.getSum(null, request, rpcCallback);
		            SumResponse response = (SumResponse) rpcCallback.get();
		            return response.hasSum() ? response.getSum() : 0L;
		        }
		    });
		    for (Long sum : results.values()) {
		        System.out.println("Sum = " + sum);
		    }
		} catch (ServiceException e) {
		e.printStackTrace();
		} catch (Throwable e) {
		    e.printStackTrace();
		}
	}
}
