package HBaseIA.TwitBase.coprocessors;

import static HBaseIA.TwitBase.hbase.RelationsDAO.FOLLOWS_TABLE_NAME;
import static HBaseIA.TwitBase.hbase.RelationsDAO.FROM;
import static HBaseIA.TwitBase.hbase.RelationsDAO.RELATION_FAM;
import static HBaseIA.TwitBase.hbase.RelationsDAO.TO;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
/* 
 * Observer 协处理器
 * 动态部署:
 * 1 打包程序至每个regionserver上
 * 2 在hbase shell执行如下命令
 * disable 'follows'
 * alter 'follows',METHOD => 'table_att','coprocessor'=>'file:///etc/hbase/conf.cloudera.hbase/coprocessor/twitbase-1.0.0.jar|HBaseIA.TwitBase.coprocessors.FollowsObserver|1001|'
 * enable 'follows'
 * 3 执行
 * 
 * Endpoint 协处理器
 * 新版本需要通过google的 protobuf实现,具体查看http://hbase.apache.org/book.html
 */
import HBaseIA.TwitBase.hbase.RelationsDAO;

/*
 * endpoint 使用了ProtoBufs
 */
public class FollowsObserver extends BaseRegionObserver {

	private Connection connection = null;

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		connection = ConnectionFactory.createConnection(env.getConfiguration());
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		connection.close();
	}

	@Override
	public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e, 
		      final Put put, final WALEdit edit, final Durability durability) throws IOException {

		TableName table = e.getEnvironment().getRegion().getRegionInfo().getTable();
		if (!Bytes.equals(table.getName(), FOLLOWS_TABLE_NAME))
			return;

		if(!put.getFamilyCellMap().containsKey(RELATION_FAM)) {
			return;
		}
		
		Cell cell = put.get(RELATION_FAM, FROM).get(0);
		String from = Bytes.toString(CellUtil.cloneValue(cell));
		cell = put.get(RELATION_FAM, TO).get(0);
		String to = Bytes.toString(CellUtil.cloneValue(cell));

		RelationsDAO relations = new RelationsDAO(connection);
		relations.addFollowedBy(to, from);
	}
}
