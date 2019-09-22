package com.allen.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2017/6/5.
 */
public class OrderProduct implements WritableComparable<OrderProduct>{
    private String order_id;
    private String pdt_id;
    private double money;

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }
    public String getPdt_id() {
        return pdt_id;
    }
    public void setPdt_id(String pdt_id) {
        this.pdt_id = pdt_id;
    }
    public double getMoney() {
        return money;
    }
    public void setMoney(double money) {
        this.money = money;
    }

    public int compareTo(OrderProduct o) {
        //不能直接这样写，因为这样的话，那么价格相同的也会被当成同一组了
        //相当于是先根据order_id进行排序，再根据money进行排序
        //必须先根据order_id排序，使得相同的order_id的对象在发送到reduce端的时候是连在一起的
        //因为之后的groupComparator的时候，是一个一个的跟后面的比较的，返回0，就认为是在同一个组中的
        //返回不为0就不是同一个组
        if(this.getOrder_id().compareTo(o.getOrder_id())==0){
            return Double.valueOf(money).compareTo(o.getMoney());
        }
        else return this.getOrder_id().compareTo(o.getOrder_id());

        /*return Double.valueOf(money).compareTo(o.getMoney());*/
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(order_id);
        out.writeUTF(pdt_id);
        out.writeDouble(money);
    }

    public void readFields(DataInput in) throws IOException {
        order_id = in.readUTF();
        pdt_id = in.readUTF();
        money = in.readDouble();
    }

    @Override
    public String toString() {
        return "OrderProduct{" +
                "order_id='" + order_id + '\'' +
                ", pdt_id='" + pdt_id + '\'' +
                ", money=" + money +
                '}';
    }
}
