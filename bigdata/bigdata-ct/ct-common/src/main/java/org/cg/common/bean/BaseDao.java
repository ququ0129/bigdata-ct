package org.cg.common.bean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.cg.common.api.Column;
import org.cg.common.api.Rowkey;
import org.cg.common.api.TableRef;
import org.cg.common.constant.Names;
import org.cg.common.constant.ValueConstant;
import org.cg.common.util.DateUtil;

import java.lang.reflect.Field;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public abstract class BaseDao {
    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start() throws Exception{
        getConnection();
        getAdmin();
    }

    protected void end() throws Exception{
        Admin admin = getAdmin();
        if(admin != null){
            admin.close();
            adminHolder.remove();
        }
        Connection conn = getConnection();
        if(conn != null){
            conn.close();
            connHolder.remove();
        }
    }

    /*
    get connection object
     */
    protected synchronized Connection getConnection() throws Exception{
        Connection conn = connHolder.get();
        if(conn == null){
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }

    /*
    get admin object
     */
    protected synchronized Admin getAdmin() throws Exception{
        Admin admin = adminHolder.get();
        if(admin == null){
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    protected void createNamespaceNX(String namespace) throws Exception{
        Admin admin = getAdmin();
        try {
            admin.getNamespaceDescriptor(namespace);
        }catch (NamespaceNotFoundException e){
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    protected void createTableXX(String name,String... families) throws Exception{
        createTableXX(name,null,null,families);
    }
    protected void createTableXX(String name,String coprocessorClass,Integer regionCount,String... families) throws Exception{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);

        if(admin.tableExists(tableName)){
            //delete table
            deleteTable(name);
        }

        //create table
        createTable(name,coprocessorClass,regionCount,families);
    }

    protected void deleteTable(String name) throws Exception{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    private void createTable(String name,String coprocessorClass,Integer regionCount,String... families) throws Exception{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        if(families == null || families.length == 0){
            families = new String[1];
            families[0] = Names.CF_INFO.getValue();
        }
        for (String family : families) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }

        if(coprocessorClass != null && !"".equals(coprocessorClass)){
            tableDescriptor.addCoprocessor(coprocessorClass);
        }

        if(regionCount == null || regionCount <= 1){
            admin.createTable(tableDescriptor);
        }else {
            byte[][] splitKeys = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor,splitKeys);
        }
    }

    private byte[][] genSplitKeys(Integer regionCount){
        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];
        List<byte[]> bsList = new ArrayList<byte[]>();
        for(int i = 0; i < splitKeyCount; i++){
            String splitKey = i + "|";
            //System.out.println(splitKey);
            bsList.add(Bytes.toBytes(splitKey));
        }
        bsList.toArray(bs);
        return bs;
    }


    protected void putData(Object obj) throws Exception{
        //Reflection
        //get tableName
        Class clazz = obj.getClass();
        TableRef tableRef = (TableRef) clazz.getAnnotation(TableRef.class);
        String tableName=tableRef.value();

        //get row key
        String stringRowkey="";
        Field[] fs = clazz.getDeclaredFields();
        for (Field f : fs) {
            Rowkey rowkey = f.getAnnotation(Rowkey.class);
            if(rowkey != null){
                f.setAccessible(true);
                stringRowkey = (String) f.get(obj);
                break;
            }
        }

        //create put object
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(stringRowkey));

        for (Field f : fs) {
            Column column = f.getAnnotation(Column.class);
            if(column != null){
                String family = column.family();
                String colName = column.column();
                if(colName == null || "".equals(colName)){
                    colName  =f.getName();
                }
                f.setAccessible(true);
                String value = (String) f.get(obj);
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(colName),Bytes.toBytes(value));
            }
        }

        //insert
        table.put(put);
        //close
        table.close();

    }

    protected void putData(String name, Put put) throws Exception{
        //get table
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));
        //insert
        table.put(put);
        //close
        table.close();
    }

    protected void putData(String name, List<Put> puts) throws Exception{
        //get table
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));
        //insert
        table.put(puts);
        //close
        table.close();
    }

    protected int genRegionNumber(String tel, String date){
        String usercode = tel.substring(tel.length()-4);
        String yearMonth = date.substring(0,6);

        int userCodeHash = usercode.hashCode();
        int yearMonthHash = yearMonth.hashCode();

        // crc algorithm
        int crc  = Math.abs(userCodeHash ^ yearMonthHash);
        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;
    }

    protected List<String[]> getStartStopRowkeys(String tel,String start,String end){
        List<String[]> rowkeyss = new ArrayList<String[]>();

        String startTime = start.substring(0,6);
        String endTime = end.substring(0,6);

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DateUtil.parse(startTime,"yyyyMM"));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime,"yyyyMM"));

        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()){
            String nowTime = DateUtil.format(startCal.getTime(),"yyyyMM");
            int regionNumber = genRegionNumber(tel,nowTime);

            String startRow = regionNumber + "_" + tel + "_" + nowTime;
            String stopRow = startRow + "|";
            String[] rowkeys = {startRow,stopRow};
            rowkeyss.add(rowkeys);
            startCal.add(Calendar.MONTH,1);
        }
        return rowkeyss;
    }
}
