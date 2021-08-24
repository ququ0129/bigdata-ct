package org.cg.producer.io;

import org.cg.common.bean.Data;
import org.cg.common.bean.DataIn;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/*
local data input
 */
public class LocalFileDataIn implements DataIn {

    private BufferedReader reader =  null;

    public LocalFileDataIn(String path){
        setPath(path);
    }
    @Override
    public void setPath(String path) {
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),
                    "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        if(reader != null){
            reader.close();
        }
    }

    @Override
    public Object read() throws IOException {
        return null;
    }

    /*
    return data set
     */
    @Override
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {
        List<T> ts = new ArrayList<T>();
        try {
            //read data
            String line = null;
            while ((line = reader.readLine()) != null){
                //convert data type
                T t = clazz.newInstance();
                t.setValue(line);
                ts.add(t);
            }

        }catch (Exception e){
            e.printStackTrace();
        }


        return ts;
    }
}
