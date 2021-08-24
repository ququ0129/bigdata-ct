package org.cg.producer.bean;

import org.cg.common.bean.Data;

public class Contact extends Data {
    private String tel;
    private String name;

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setValue(Object val) {
        content = (String) val;
        String[] values = content.split("\t");
        setTel(values[0]);
        setName(values[1]);
    }

    @Override
    public String toString() {
        return "Contact["+tel+","+name+"]";
    }
}
