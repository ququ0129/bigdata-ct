package org.cg.producer.io;

import org.cg.common.bean.DataOut;

import java.io.*;

/*
local data output
 */
public class LocalFileDataOut implements DataOut {
    private PrintWriter writer = null;
    public LocalFileDataOut(String path){
        setPath(path);
    }

    @Override
    public void write(Object data) throws IOException {
        write(data.toString());
    }

    @Override
    public void write(String data) throws IOException {
        writer.println(data);
        writer.flush();
    }

    @Override
    public void setPath(String path) {
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path),"UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws IOException {
        if(writer != null){
            writer.close();
        }
    }
}
