package com.qinh.case2;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @author Qh
 * @version 1.0
 * @date 2021-08-24-23:35
 */
public class DistributedLockTest {
    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        final DistributedLock lock1 = new DistributedLock();

        final DistributedLock lock2 = new DistributedLock();

        new Thread(() -> {
            try {
                lock1.zkLock();
                System.out.println("线程1 启动，获取锁");
                Thread.sleep(5 * 10000);
                lock1.unZkLock();
                System.out.println("线程1 释放锁");
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();


        new Thread(() -> {
            try {
                lock2.zkLock();
                System.out.println("线程2 启动，获取锁");
                Thread.sleep(5 * 10000);
                lock2.unZkLock();
                System.out.println("线程2 释放锁");
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }
}
