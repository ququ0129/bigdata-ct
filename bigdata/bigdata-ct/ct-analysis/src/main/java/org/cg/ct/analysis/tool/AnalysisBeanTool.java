package org.cg.ct.analysis.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;
import org.cg.common.constant.Names;
import org.cg.ct.analysis.io.MySQLBeanOutputFormat;
import org.cg.ct.analysis.io.MySQLOutputFormat;
import org.cg.ct.analysis.kv.AnalysisKey;
import org.cg.ct.analysis.kv.AnalysisValue;
import org.cg.ct.analysis.mapper.AnalysisBeanMapper;
import org.cg.ct.analysis.mapper.AnalysisTextMapper;
import org.cg.ct.analysis.reducer.AnalysisBeanReducer;
import org.cg.ct.analysis.reducer.AnalysisTextReducer;

public class AnalysisBeanTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(AnalysisBeanTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));

        //mapper
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getValue(),
                scan,
                AnalysisBeanMapper.class,
                AnalysisKey.class,
                Text.class,
                job
        );

        //reducer
        job.setReducerClass(AnalysisBeanReducer.class);
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);

        //outputformat
        job.setOutputFormatClass(MySQLBeanOutputFormat.class );

        boolean flg = job.waitForCompletion(true);
        if(flg){
            return JobStatus.State.SUCCEEDED.getValue();
        } else {
            return JobStatus.State.FAILED.getValue();
        }

    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
