package com.qinh.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * 分布式锁
 *
 * @author Qh
 * @version 1.0
 * @date 2021-08-24-21:37
 */
public class DistributedLock {

    //private final String connectString = "192.168.116.128:2181,192.168.116.129:2181,192.168.116.130:2181";
    private final String connectString = "192.168.116.129:2181,192.168.116.130:2181";
    private final int sessionTimeout = 400000;
    private ZooKeeper zk;

    private CountDownLatch connectLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);

    private String waitPath;
    private String currentNode;

    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        //获取连接
        zk = new ZooKeeper(connectString, sessionTimeout, watchEvent -> {
            //connectLatch 如果连接上zk,可以释放
            if (watchEvent.getState() == Watcher.Event.KeeperState.SyncConnected){
                connectLatch.countDown();
            }

            //waitLatch 需要释放
            if (watchEvent.getType() == Watcher.Event.EventType.NodeDeleted && watchEvent.getPath().equals(waitPath)){
                waitLatch.countDown();
            }
        });

        //等待zk正常连接后，往下走程序
        connectLatch.await();

        //判断根节点/locks是否存在
        Stat stat = zk.exists("/locks", false);

        if (Objects.isNull(stat)){
            //创建一下根节点
            zk.create("/locks", "locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    /**
     * 对zk加锁
     */
    public void zkLock() throws KeeperException, InterruptedException {
        //创建对应的临时带序号的节点
       currentNode = zk.create("/locks/" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        //判断创建的节点是否是最小的序号节点，如果是获取到锁，如果不是，监听当前序号前一个节点
        List<String> children = zk.getChildren("/locks", false);

        //如果children只有一个值，那就直接获取锁，如果有多个节点，需要判断，谁最小
        if (children.size() == 1){
            return;
        }else {
            Collections.sort(children);
            //获取对应的节点名称 seq-xxxxx
            String thisNode = currentNode.substring("/locks/".length());
            //通过seq-xxxx获取该节点在children集合的位置
            int index = children.indexOf(thisNode);

            //判断
            if (index == -1){
                System.out.println("数据异常");
            } else if (index == 0){
                //就一个节点，可以获取锁
                return;
            } else {
                //需要监听前一个节点变化
                waitPath = "/locks/" + children.get(index -1);
                zk.getData(waitPath,true,new Stat());

                //等待监听
                waitLatch.await();
                return;
            }

        }
    }

    /**
     * 对zk解锁
     */
    public void unZkLock() throws KeeperException, InterruptedException {
        //删除节点
        zk.delete(currentNode, -1);
    }
}
