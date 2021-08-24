package org.cg.producer;

import org.cg.common.bean.Producer;
import org.cg.producer.bean.LocalFileProducer;
import org.cg.producer.io.LocalFileDataIn;
import org.cg.producer.io.LocalFileDataOut;

public class Bootstrap {

    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            System.out.println("invalid arguments");
            System.exit(1);
        }
        //create producer
        Producer producer = new LocalFileProducer();
        String in=args[0];
        String out=args[1];
        producer.setIn(new LocalFileDataIn(in));
        producer.setOut(new LocalFileDataOut(out));

        //produce data
        producer.produce();

        //close producer
        producer.close();
    }
}
