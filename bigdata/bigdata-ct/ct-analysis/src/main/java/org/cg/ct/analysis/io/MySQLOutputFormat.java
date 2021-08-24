package org.cg.ct.analysis.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.cg.common.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MySQLOutputFormat extends OutputFormat<Text,Text> {

    protected static class MySQLRecordWriter extends RecordWriter<Text, Text> {
        public Connection conn = null;
        private Jedis jedis = null;

        public MySQLRecordWriter() {
            conn = JDBCUtil.getConnection();
            jedis = new Jedis("master",6379);

        }

        @Override
        public void write(Text text, Text text2) throws IOException, InterruptedException {
            PreparedStatement statement = null;

            String[] s = text2.toString().split("_");
            String sumcall = s[0];
            String sumduration = s[1];
            try {
                String insertSQL = "insert into ct_call(telid,dateid,sumcall,sumduration)values(?,?,?,?)";
                statement = conn.prepareStatement(insertSQL);
                String k = text.toString();
                String[] ks = k.split("_");

                String tel = ks[0];
                String date = ks[1];

                statement.setInt(1,Integer.parseInt(jedis.hget("ct_user",tel)));
                statement.setInt(2,Integer.parseInt(jedis.hget("ct_date",date)));
                statement.setInt(3,Integer.parseInt(sumcall));
                statement.setInt(4,Integer.parseInt(sumduration));
                statement.executeUpdate();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }finally {
                if (statement != null){
                    try {
                        statement.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MySQLRecordWriter();

    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    private FileOutputCommitter committer = null;

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(taskAttemptContext);
            committer = new FileOutputCommitter(output, taskAttemptContext);
        }
        return committer;
    }
}