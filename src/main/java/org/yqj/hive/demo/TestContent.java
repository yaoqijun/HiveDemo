package org.yqj.hive.demo;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * Created by yaoqijun.
 * Date:2015-10-29
 * Email:yaoqj@terminus.iove
 * Descirbe:
 */
public class TestContent {



    public static void main(String []args) throws Exception{
        System.out.println("test hive content");
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("terminus-hive-context.xml");
//        HiveTemplate hiveTemplate = context.getBean(HiveTemplate.class);
        JdbcTemplate template = context.getBean(JdbcTemplate.class);
//        List<Map<String,Object>> result = template.queryForList("select * from user_test");
//        System.out.println(result.toString());

        String sql = "";
        //  test get database info, get tables info
        //List<String> result = hiveTemplate.query("show tables");
        //List<Map<String,Object>> result = template.queryForList("show databases");
        //System.out.println(result.toString());

        //load data
//        sql = "load data inpath '/user/yaoqijun/part-m-00000' into table throw_test2";
//        template.execute(sql);

//        sql = "insert overwrite table throw_test select * from throw_test where 1=0";
//        template.execute(sql);

//        createTable(template);
//        testInsert(template);
//        testDelete(template);
        testSelect(template);
//        showTables(template);
        System.out.println("finish");
    }

    public static void createTable(JdbcTemplate template){
        //test table execute
        String sql ="";
        sql = "create table testTableNew(id int ,name string ) clustered by (id) into 2 buckets stored as orc TBLPROPERTIES('transactional'='true')";
        template.execute(sql);
    }

    public static void testInsert(JdbcTemplate template) throws Exception{
        //test insert haha
        String sql = "";
        //sql = "insert into table farm_xrnm_infos values ('1a26d37a-e904-483a-a63d-50cd4da50f2c','淇县亿源牧业开发有限公司',null,null,null,null)";
        sql = "insert into table test VALUES (1,'yaoqijun',1)";
        sql = new String(sql.getBytes("UTF-8"),"ISO-8859-1");
//        sql = "insert into table stu select * from stu where";
//        sql = "insert into table stu select 999,'呵呵' from stu";
//        sql = "INSERT overwrite TABLE stu select * from stu where 1=0";
        System.out.println(sql);
        template.execute(sql);
    }

    public static void testDelete(JdbcTemplate template) throws Exception{
        String sql = "c";
        template.execute(sql);
    }

    private static void testSelect (JdbcTemplate template) throws Exception{
        String sql ="";
        //sql = " SELECT  null, to_date(s.EventDate), s.FarmOID, 'efd038bb-a823-4420-9f3d-5b05be34d132', COUNT(s.OID) as OutCount, SUM(s.EWeight) as OutWeight, null   FROM view_EventListGain as s   LEFT JOIN TB_FieldValue as c   ON s.ChgType = c.ColID   LEFT JOIN view_PigLocationList as p   ON s.EventLocation = p.OID   WHERE EventEName = 'GainChange'   AND trim(c.TypeID) = '变动类型'   AND trim(c.Remark) = '计入出栏猪'   AND trim(p.TypeName) = '保育猪'   GROUP BY s.FarmOID, to_date(s.EventDate)";
        sql = "select * from test";
        System.out.println(sql);
        List<Map<String,Object>> result = template.queryForList(sql);
        System.out.println(result);
    }

    private static void showTables(JdbcTemplate template){
        String sql ="";

        sql = "show tables";
        List<Map<String,Object>> result = template.queryForList(sql);
        System.out.println(result);
    }
}
