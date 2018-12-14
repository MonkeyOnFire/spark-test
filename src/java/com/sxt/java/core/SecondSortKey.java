package com.sxt.java.core;

import java.io.Serializable;

/**
 * Created by Layne on 2018/5/4.
 */
public class SecondSortKey  implements Serializable,Comparable<SecondSortKey>{
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private int first;
    private int second;
    public int getFirst() {
        return first;
    }
    public void setFirst(int first) {
        this.first = first;
    }
    public int getSecond() {
        return second;
    }
    public void setSecond(int second) {
        this.second = second;
    }
    public SecondSortKey(int first, int second) {
        super();
        this.first = first;
        this.second = second;
    }
    @Override
    public int compareTo(SecondSortKey o1) {
        if (getFirst() - o1.getFirst() == 0) {
            return getSecond() - o1.getSecond();

            /*if(getSecond() - o1.getSecond() == 0) {
                return getThree() - o1.getThree();
            }else {
                return getSecond() - o1.getSecond();
            }*/

        } else {
            return getFirst() - o1.getFirst();
        }

    }
}