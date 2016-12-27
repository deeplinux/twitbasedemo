package HBaseIA.TwitBase.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import HBaseIA.TwitBase.coprocessors.Sum.SumRequest;
import HBaseIA.TwitBase.coprocessors.Sum.SumResponse;
import HBaseIA.TwitBase.coprocessors.Sum.SumService;

public class SumEndPoint extends SumService implements Coprocessor, CoprocessorService {

    private RegionCoprocessorEnvironment env;

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // do mothing
    }

    @Override
    public void getSum(RpcController controller, SumRequest request, RpcCallback done) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(request.getFamily()));
        scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes(request.getColumn()));
        SumResponse response = null;
        InternalScanner scanner = null;
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;
                        long sum = 0L;
                do {
                        hasMore = scanner.next(results);
                        for (Cell cell : results) {
                            sum = sum + 1;
                     }
                        results.clear();
                } while (hasMore);

                response = SumResponse.newBuilder().setSum(sum).build();

        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {}
            }
        }
        done.run(response);
    }
}