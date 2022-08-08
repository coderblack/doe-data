package cn.doitedu.datacollect.doris;

import java.io.Serializable;

public class Stu implements Serializable {
    private int id;
    private byte age;
    private String name;

    public Stu() {
    }

    public Stu(int id, byte age, String name) {
        this.id = id;
        this.age = age;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public byte getAge() {
        return age;
    }

    public void setAge(byte age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
