package org.cg.producer.bean;

import org.cg.common.bean.DataIn;
import org.cg.common.bean.DataOut;
import org.cg.common.bean.Producer;
import org.cg.common.util.DateUtil;
import org.cg.common.util.NumberUtil;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

/*
local data file producer
 */
public class LocalFileProducer implements Producer {
    private DataIn in;
    private DataOut out;
    private volatile boolean flg=true;
    @Override
    public void setIn(DataIn in) {
        this.in=in;
    }

    @Override
    public void setOut(DataOut out) {
        this.out=out;
    }

    @Override
    public void produce() {
        try {
            //read data from the local file
            List<Contact> contacts = in.read(Contact.class);

            while (flg){
                //find 2 telephone number randomly(calling number and called number)
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index;
                while (true) {
                    call2Index= new Random().nextInt(contacts.size());
                    if (call1Index != call2Index) {
                        break;
                    }
                }
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                //generate random talk time
                String startDate = "20180101000000";
                String endDate = "20190101000000";
                long startTime= DateUtil.parse(startDate,"yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate,"yyyyMMddHHmmss").getTime();
                long callTime = startTime + (long)((endTime-startTime)*Math.random());
                String callTimeString=DateUtil.format(new Date(callTime),"yyyyMMddHHmmss");

                //generate random call duration
                String duration = NumberUtil.format(new Random().nextInt(3000),4);

                //generate call logs
                CallLog log = new CallLog(call1.getTel(),call2.getTel(),callTimeString,duration);

                //test data format
                //System.out.println(log);

                //save the call logs
                out.write(log);

                Thread.sleep(500);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        if(in != null){
            in.close();
        }
        if(out != null){
            out.close();
        }
    }
}
