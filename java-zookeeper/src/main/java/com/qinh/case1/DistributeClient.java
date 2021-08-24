package com.qinh.case1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 创建Zookeeper客户端
 *
 * @author Qh
 * @version 1.0
 * @date 2021/8/24 16:00
 */
public class DistributeClient {

    //private final String connectString = "192.168.116.128:2181,192.168.116.129:2181,192.168.116.130:2181";
    private final String connectString = "192.168.30.129:2181,192.168.30.130:2181,192.168.30.131:2181";
    private final int sessionTimeout = 400000;
    private ZooKeeper zk;

    public static void main(String[] args) throws Exception {
        DistributeClient client = new DistributeClient();
        //1.获取zk连接
        client.getConnect();
        //2.监听/servers下面节点的新增和删除
        client.getServerList();

        //3.业务逻辑
        client.business();
    }

    private void business() throws InterruptedException {
        while (true){
        }
    }

    /**
     * 监听指定节点的变化
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void getServerList() throws KeeperException, InterruptedException {
        //注册监听
        List<String> children = zk.getChildren("/servers", true);
        //遍历节点的值即主机的host
        List<String> servers = new ArrayList<>();
        for (String child : children){
            byte[] data = zk.getData("/servers/" + child, false, null);
            servers.add(new String(data));
        }
        servers.forEach(System.out::println);
    }

    private void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, watchEvent -> {
            try {
                //一直监听Zookeeper节点的变化
                getServerList();
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
