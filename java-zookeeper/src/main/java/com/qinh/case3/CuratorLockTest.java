package com.qinh.case3;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

/**
 * Curator框架的测试
 *
 * @author Qh
 * @version 1.0
 * @date 2021/8/25 16:56
 */
public class CuratorLockTest {
    
    public static void main(String[] args){

        //创建分布式锁1
        InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), "/locks");
        
        //创建分布式锁2
        InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), "/locks");

        new Thread(()->{
            try {
                lock1.acquire();
                System.out.println("线程1获取到锁");
                //可重入锁
                lock1.acquire();
                System.out.println("线程1再次获取到锁");
                TimeUnit.SECONDS.sleep(5);
                lock1.release();
                System.out.println("线程1释放锁");
                lock1.release();
                System.out.println("线程1再次释放锁");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(()->{
            try {
                lock2.acquire();
                System.out.println("线程2获取到锁");
                lock2.acquire();
                System.out.println("线程2再次获取到锁");
                TimeUnit.SECONDS.sleep(5);
                lock2.release();
                System.out.println("线程2释放锁");
                lock2.release();
                System.out.println("线程2再次释放锁");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private static CuratorFramework getCuratorFramework() {

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(3000, 3);

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("192.168.30.129:2181,192.168.30.130:2181,192.168.30.131:2181")
                .connectionTimeoutMs(400000)
                .sessionTimeoutMs(400000)
                .retryPolicy(retry).build();

        //启动客户端
        client.start();

        System.out.println("zookeeper客户端启动成功");
        return client;
    }
}
