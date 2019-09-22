package com.allen.hadoop.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class InfoBean implements Writable{
	//join的字段
	private String id;
	private String name;
	private String sex;

	
	private String cname;
	private String score;
	
	private String flag;


	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeUTF(name);
		out.writeUTF(sex);
		out.writeUTF(cname);
		out.writeUTF(score);
		out.writeUTF(flag);
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readUTF();
		name = in.readUTF();
		sex = in.readUTF();
		cname = in.readUTF();
		score = in.readUTF();
		flag = in.readUTF();
	}


	public InfoBean setInfoBean(String id, String name, String sex, String cname, String score, String flag) {
		this.id = id;
		this.name = name;
		this.sex = sex;
		this.cname = cname;
		this.score = score;
		this.flag = flag;
		return this;
	}


	public InfoBean(String id, String name, String sex, String cname, String score, String flag) {
		this.id = id;
		this.name = name;
		this.sex = sex;
		this.cname = cname;
		this.score = score;
		this.flag = flag;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getCname() {
		return cname;
	}

	public void setCname(String cname) {
		this.cname = cname;
	}

	public String getScore() {
		return score;
	}

	public void setScore(String score) {
		this.score = score;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public InfoBean() {}

	@Override
	public String toString() {
		return "InfoBean{" +
				"id='" + id + '\'' +
				", name='" + name + '\'' +
				", sex='" + sex + '\'' +
				", cname='" + cname + '\'' +
				", score='" + score + '\'' +
				", flag='" + flag + '\'' +
				'}';
	}
}
