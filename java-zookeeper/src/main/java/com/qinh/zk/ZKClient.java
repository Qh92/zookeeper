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
    //private final String connectString = "192.168.116.128:2181,192.168.116.129:2181,192.168.116.130:2181";
    private final String connectString = "192.168.30.129:2181,192.168.30.130:2181,192.168.30.131:2181";
    private final int sessionTimeout = 400000;

    private ZooKeeper zkClient;

    @Test
    @Before
    public void init() throws IOException {
        //连接Zookeeper服务端
        zkClient = new ZooKeeper(connectString, sessionTimeout, watchedEvent -> {
            //监听事件,收到事件通知后的回调函数（用户的业务逻辑）
            System.out.println("-------------------");
            List<String> children = null;
            try {
                //监听根节点的变化,再次启动监听
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
        //参数 1：要创建的节点的路径； 参数 2：节点数据 ； 参数 3：节点权限 ；参数 4：节点的类型
        String nodeCreated = zkClient.create("/qinhao", "allen".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 监听节点的增加或者减少
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        // watch=true表示调用构造器里面的监听事件即 上面 new ZooKeeper(connectString, sessionTimeout, watchedEvent -> {})中的监听事件
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
