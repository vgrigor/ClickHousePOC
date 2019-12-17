package ru.td.ch.util;

public class Timer{

    private long time0;
    static public Timer instance(){
        return new Timer();
    }

    public  Timer start(){
        time0 = System.nanoTime();
        return this;
    }

    /**
     * @return   Milliseconds
     */
    public  long end(String msg){
        long time1 = System.nanoTime();

        Long mSec = (time1 - time0)/1_000_000;
        time0 = System.nanoTime();
        System.out.print("Time= " + mSec/1_000 +"," + mSec % 1_000 +" seconds" );

        if(msg != null && msg.length() > 0 )
            System.out.println(" OP:" + msg);
        else
            System.out.println("");


        return mSec;
    }

    public  long end() {
        return end("");
    }
}
