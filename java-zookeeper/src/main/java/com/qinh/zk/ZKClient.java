package com.qinh.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * zookeeper客户端
 *
 * @author Qh
 * @version 1.0
 * @date 2021-08-23-22:55
 */
public class ZKClient {

    /** 注意：逗号左右不能有空格 */
    private final String connectString = "192.168.116.128:2181,192.168.116.129:2181,192.168.116.130:2181";
    private final int sessionTimeout = 400000;

    private ZooKeeper zkClient;

    @Test
    @Before
    public void init() throws IOException {
        //连接Zookeeper服务端
        zkClient = new ZooKeeper(connectString, sessionTimeout, watchedEvent -> {
            //监听事件
            System.out.println("-------------------");
            List<String> children = null;
            try {
                //监听根节点的变化
                children = zkClient.getChildren("/", true);
                children.forEach(System.out::println);
                System.out.println("-----------------------");
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * 创建子节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        String nodeCreated = zkClient.create("/qinhao2", "allen2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 监听节点的增加或者减少
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        // watch=true表示调用构造器里面的监听事件
        List<String> children = zkClient.getChildren("/", true);
        //children.forEach(System.out::println);
        //延时,让其一直处于监听状态
        while (true){
        }
    }

    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/qinhao", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }
}
