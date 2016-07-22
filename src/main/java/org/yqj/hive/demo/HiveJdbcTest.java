package org.yqj.hive.demo;

import org.apache.hive.jdbc.HiveStatement;

import java.sql.*;

/**
 * Created by yaoqijun.
 * Date:2015-10-26
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class HiveJdbcTest {

    private static final String DRIVERNAME = "org.apache.hive.jdbc.HiveDriver";

    static Statement stmt = null;

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
            sql = new String(sql.getBytes("UTF-8"), "ISO-8859-1");
            stmt = connection.createStatement();
            sql = "insert into test values ('hehe', 10)";
            sql = "select * from test";

//            new GetLogThread().start();

//            for(int i=0; i<10; i++) {
            ResultSet resultSet = stmt.executeQuery(sql);
            outputResultSet(resultSet);
//                System.out.println("****************** finish execute once log info");
//                System.out.println("****************** start log info output");
//                ((HiveStatement) stmt).getQueryLog().forEach(s->{
//                    System.out.println(s);
//                });
//                System.out.println("************** finish log info output content");
//                outputResultSet(resultSet);
                Thread.currentThread().sleep(1000l);
//            }
//            System.out.println(statement.execute(sql));
            stmt.close();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("load hive data exception");
        }
    }

    public static void outputResultSet(ResultSet resultSet) throws Exception {
        System.out.println("********************* result set info is");

        ResultSetMetaData data = resultSet.getMetaData();

        while (resultSet.next()){
            System.out.print(resultSet.getString(1) + "   ");
            System.out.println(resultSet.getString(2));
        }

        Integer count = data.getColumnCount();
        for(int i=1; i<=count; i++){
            System.out.println(data.getColumnName(i));
        }

        System.out.println("********************* result set info is over");
    }

    static class GetLogThread extends Thread {

        public void run() { //真生的输出运行进度的thread
            if (stmt == null) {
                return;
            }
            HiveStatement hiveStatement = (HiveStatement) stmt;
            try {
                while (! hiveStatement.isClosed()){
                    try {
                        System.out.println("*** to fetch log info");
                        for (String s : hiveStatement.getQueryLog()) {
                            System.out.println(s);
                        }
                        System.out.println("*** over fetch log info");
                        Thread.currentThread().sleep(1000L);
                    } catch (SQLException e) { //防止while里面报错，导致一直退不出循环
                        e.printStackTrace();
                        return;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return;
                    }
                }
                System.out.println("** over");
            } catch (SQLException e) {
                System.out.println("test condition");
                e.printStackTrace();
            }
        }
    }
}
