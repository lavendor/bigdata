package com.lavendor.hadoop.fof;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 比较朋友二度关系的类
 */
public class Friend implements WritableComparable<Friend> {
    private String friend1;
    private String friend2;
    private int hot;

    public String getFriend1() {
        return friend1;
    }

    public void setFriend1(String friend1) {
        this.friend1 = friend1;
    }

    public String getFriend2() {
        return friend2;
    }

    public void setFriend2(String friend2) {
        this.friend2 = friend2;
    }

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    @Override
    public int compareTo(Friend friend) {
        int c = this.friend1.compareTo(friend.getFriend2());
        if(c == 0){
            return Integer.compare(this.hot,friend.getHot());
        }
        return c;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.friend1);
        out.writeUTF(this.friend2);
        out.writeInt(this.hot);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.friend1 = in.readUTF();
        this.friend2 = in.readUTF();
        this.hot = in.readInt();
    }
}
