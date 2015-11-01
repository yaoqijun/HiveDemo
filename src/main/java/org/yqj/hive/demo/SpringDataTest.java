package org.yqj.hive.demo;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by yaoqijun.
 * Date:2015-10-26
 * Email:yaoqj@terminus.io
 * Descirbe:
 */
public class SpringDataTest {
    public static void main(String []args){
        System.out.println("test all");
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("beans.xml");
        //回去对应文件的内容

    }
}
