package com.qinh.case1;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Objects;

/**
 * Zookeeper服务端
 *
 * @author Qh
 * @version 1.0
 * @date 2021-08-24-0:01
 */
public class DistributeServer {

    //private final String connectString = "192.168.116.128:2181,192.168.116.129:2181,192.168.116.130:2181";
    private final String connectString = "192.168.30.129:2181,192.168.30.130:2181,192.168.30.131:2181";
    private final int sessionTimeout = 400000;
    private ZooKeeper zk;

    public static void main(String[] args) throws Exception {
        //1.获取zk连接
        DistributeServer server = new DistributeServer();
        server.getConnect();
        //2.注册服务器到zk集群
        if (Objects.isNull(args)){
            throw new RuntimeException("参数传入异常！");
        }
        for (String arg : args){
            server.regist(arg);
        }
        //3.启动业务逻辑
        server.business();
    }

    private void business() throws InterruptedException {
        while (true){
        }
    }

    /**
     * 服务注册，并创建对应的节点
     *
     * @param host
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void regist(String host) throws KeeperException, InterruptedException {
        String create = zk.create("/servers/hadoop" + host, host.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(host + " is online");
    }


    /**
     * 服务器的连接
     *
     * @throws IOException
     */
    private void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, watchEvent -> {
        });
    }
}
