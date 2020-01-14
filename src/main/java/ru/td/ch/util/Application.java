package ru.td.ch.util;

import static java.lang.Thread.sleep;

public class Application {
    public static void waitInfinitely(){
        for(;;){
            try {
                sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void wait(int mSec){
            try {
                sleep(mSec);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

    }


    public static void main(String[] args) {
        waitInfinitely();
    }
}
