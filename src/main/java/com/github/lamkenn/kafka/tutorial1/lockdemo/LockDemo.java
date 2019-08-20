package com.github.lamkenn.kafka.tutorial1.lockdemo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockDemo {

    private Lock lock = new ReentrantLock();

    private void workOn() {
        System.out.println(Thread.currentThread().getName() + ":上班!");
    }

    private void workOff() {
        System.out.println(Thread.currentThread().getName() + ":下班");
    }


    public void work() {
        try {
            lock.lock();
            workOn();
            System.out.println(Thread.currentThread().getName()
                    + "工作中!!!!");
            Thread.sleep(100);
            workOff();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        LockDemo lockDemo = new LockDemo();

        int i = 0;
        List<Thread> list = new ArrayList<>(30);
        do {
            Thread a = new Thread(new Runnable() {
                @Override
                public void run() {
                    lockDemo.work();
                }
            }, "小A_" + i);

            Thread b = new Thread(new Runnable() {
                @Override
                public void run() {
                    lockDemo.work();
                }
            }, "小B_" + i);


            list.add(a);
            list.add(b);
        } while (i++ < 10);
        list.parallelStream().forEach(Thread::start);

        Thread.sleep(3000);
        System.out.println("main over!");
    }
}