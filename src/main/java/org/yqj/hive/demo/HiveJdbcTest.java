package org.yqj.hive.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Created by yaoqijun.
 * Date:2015-10-26
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class HiveJdbcTest {

    private static final String DRIVERNAME = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String []args){
        System.out.println("hive jdbc test content");
        try{
            Class.forName(DRIVERNAME);
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("jdbc class driver load error");
            System.exit(1);
        }
        try{
            String sql = "";
            Connection connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default?useUnicode=true&characterEncoding=UTF-8", "yaoqijun", "yao4094230");
            //sql ="load data inpath '/user/yaoqijun/part-m-00000' into table test_create";
            sql = "insert into TABLE parana_addresses values(1,1,'哈哈Hive 这个傻掉',2,1)";
            sql = new String(sql.getBytes("UTF-8"), "ISO-8859-1");
            Statement statement = connection.createStatement();
            System.out.println(statement.execute(sql));
            statement.close();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("load hive data exception");
        }
    }
}
