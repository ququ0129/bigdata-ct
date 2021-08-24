package org.cg.common.bean;

import java.io.Closeable;

/*
producer interface
 */
public interface Producer extends Closeable {

    public void setIn(DataIn in);
    public void setOut(DataOut out);

    /*
    produce data
     */
    public void produce();
}
