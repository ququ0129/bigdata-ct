package org.cg.ct.consumer.dao;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.cg.common.bean.BaseDao;
import org.cg.common.constant.Names;
import org.cg.common.constant.ValueConstant;
import org.cg.ct.consumer.bean.Calllog;

import java.util.ArrayList;
import java.util.List;

public class HBaseDao extends BaseDao {
    public void init() throws Exception{
        start();
        createNamespaceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(),"org.cg.ct.consumer.coprocessor.InsertCalleeCoprocessor", ValueConstant.REGION_COUNT,Names.CF_CALLER.getValue(),Names.CF_CALLEE.getValue());
        end();
    }

    public void insertData(Calllog log) throws Exception{
        log.setRowkey(genRegionNumber(log.getCall1(), log.getCalltime())+"_"+log.getCall1()
                +"_"+log.getCalltime()+"_"+log.getCall2()+"_"+log.getDuration());
        putData(log);

    }
    public void insertData(String value) throws Exception{
        //get call log data
        String[] values = value.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        //calling
        //create put  object
        //row key design
        String rowkey = genRegionNumber(call1,calltime)+"_"+call1+"_"+calltime+"_"+call2+"_"+duration+"_1";
        Put put = new Put(Bytes.toBytes(rowkey));
        byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());
        put.addColumn(family,Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(family,Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(family,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(family,Bytes.toBytes("duration"),Bytes.toBytes(duration));
        put.addColumn(family,Bytes.toBytes("flg"),Bytes.toBytes("1"));

        //called
//        String calledRowkey = genRegionNumber(call2,calltime)+"_"+call2+"_"+calltime+"_"+call1+"_"+duration+"_0";
//        Put calledPut = new Put(Bytes.toBytes(calledRowkey));
//        byte[] calledFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
//        calledPut.addColumn(calledFamily,Bytes.toBytes("call1"),Bytes.toBytes(call2));
//        calledPut.addColumn(calledFamily,Bytes.toBytes("call2"),Bytes.toBytes(call1));
//        calledPut.addColumn(calledFamily,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
//        calledPut.addColumn(calledFamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));
//        calledPut.addColumn(calledFamily,Bytes.toBytes("flg"),Bytes.toBytes("0"));

        List<Put> puts = new ArrayList<Put>();
        puts.add(put);
        //puts.add(calledPut);

        putData(Names.TABLE.getValue(),puts);

    }
}
