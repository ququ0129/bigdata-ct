package org.cg.common.util;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;

public class JDBCUtil {
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://172.18.0.2:3306/ct?useUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_USERNAME = "admin";
    private static final String MYSQL_PASSWORD = "admin";

    public static Connection getConnection(){
        Connection conn = null;
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            conn = DriverManager.getConnection(MYSQL_URL,MYSQL_USERNAME,MYSQL_PASSWORD);
        }catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }

    public static void main(String[] args) {
        System.out.println(getConnection());
    }
}
