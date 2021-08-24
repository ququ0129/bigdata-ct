package org.cg.ct.consumer.coprocessor;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.cg.common.bean.BaseDao;
import org.cg.common.constant.Names;

import java.io.IOException;

/*
use coprocessor to insert data
1. create coprocessor class
2. correlate coprocessor class
 */
public class InsertCalleeCoprocessor extends BaseRegionObserver {
    //HBase auto insert called data after insert calling data
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //get table
        Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABLE.getValue()));

        //save data
        String callingRowkey=Bytes.toString(put.getRow());
        String[] values = callingRowkey.split("_");
        String call1=values[1];
        String call2=values[3];
        String calltime=values[2];
        String duration=values[4];
        String flg = values[5];

        if("1".equals(flg)){
            CoprocessorDao coprocessorDao = new CoprocessorDao();
            String calledRowkey = coprocessorDao.getRegionNumber(call2,calltime)
                    +"_"+call2+"_"+calltime+"_"+call1+"_"+duration+"_0";

            Put calledPut = new Put(Bytes.toBytes(calledRowkey));
            byte[] calledFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
            calledPut.addColumn(calledFamily,Bytes.toBytes("call1"),Bytes.toBytes(call2));
            calledPut.addColumn(calledFamily,Bytes.toBytes("call2"),Bytes.toBytes(call1));
            calledPut.addColumn(calledFamily,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
            calledPut.addColumn(calledFamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));
            calledPut.addColumn(calledFamily,Bytes.toBytes("flg"),Bytes.toBytes("0"));

            table.put(calledPut);

            //close table
            table.close();
        }
    }
    private class CoprocessorDao extends BaseDao{
        public int getRegionNumber(String tel,String time){
            return genRegionNumber(tel,time);
        }
    }
}
