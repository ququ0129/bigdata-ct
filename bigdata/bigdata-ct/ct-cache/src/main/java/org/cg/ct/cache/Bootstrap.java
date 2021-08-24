package org.cg.ct.cache;

import org.cg.common.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Bootstrap {
    public static void main(String[] args) {

        //mysql
        Connection conn = null;

        Map<String,Integer> userMap = new HashMap<String,Integer>();
        Map<String,Integer> dateMap = new HashMap<String,Integer>();

        //get user data, date data

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            conn = JDBCUtil.getConnection();
            String queryUserSql = "select id,tel from ct_user";
            preparedStatement = conn.prepareStatement(queryUserSql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                Integer id = resultSet.getInt(1);
                String tel = resultSet.getString(2);
                userMap.put(tel,id);
            }

            resultSet.close();

            String queryDateSql = "select id,year,month,day from ct_date";
            preparedStatement = conn.prepareStatement(queryDateSql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                Integer id = resultSet.getInt(1);
                String year = resultSet.getString(2);
                String month = resultSet.getString(3);
                String day = resultSet.getString(4);

                if ( month.length() == 1 ) {
                    month = "0" + month;
                }
                if ( day.length() == 1 ) {
                    day = "0" + day;
                }
                dateMap.put(year+month+day,id);
            }

            resultSet.close();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if (resultSet != null){
                try {
                    resultSet.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if(preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        //System.out.println(userMap.size());
        //System.out.println(dateMap.size());

        Jedis jedis = new Jedis("master",6379);

        Iterator<String> keyIterator = userMap.keySet().iterator();
        while (keyIterator.hasNext()){
            String key  = keyIterator.next();
            Integer value = userMap.get(key);
            jedis.hset("ct_user",key,""+value);
        }
        keyIterator = dateMap.keySet().iterator();
        while (keyIterator.hasNext()){
            String key  = keyIterator.next();
            Integer value = dateMap.get(key);
            jedis.hset("ct_date",key,""+value);
        }
    }
}
