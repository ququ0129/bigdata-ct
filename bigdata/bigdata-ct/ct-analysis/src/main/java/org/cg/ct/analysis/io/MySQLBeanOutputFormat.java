package org.cg.ct.analysis.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.cg.common.util.JDBCUtil;
import org.cg.ct.analysis.kv.AnalysisKey;
import org.cg.ct.analysis.kv.AnalysisValue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MySQLBeanOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue> {

    protected static class MySQLRecordWriter extends RecordWriter<AnalysisKey, AnalysisValue> {
        public Connection conn = null;

        private Map<String,Integer> userMap = new HashMap<String,Integer>();
        private Map<String,Integer> dateMap = new HashMap<String,Integer>();

        public MySQLRecordWriter() {
            conn = JDBCUtil.getConnection();

            //get user data, date data

            PreparedStatement preparedStatement = null;
            ResultSet resultSet = null;

            try {
                String queryUserSql = "select id,tel from ct_user";
                preparedStatement = conn.prepareStatement(queryUserSql);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()){
                    Integer id = resultSet.getInt(1);
                    String tel = resultSet.getString(2);
                    userMap.put(tel,id);
                }

                resultSet.close();

                String queryDateSql = "select id,year,month,day from ct_date";
                preparedStatement = conn.prepareStatement(queryDateSql);
                resultSet = preparedStatement.executeQuery();
                while (resultSet.next()){
                    Integer id = resultSet.getInt(1);
                    String year = resultSet.getString(2);
                    String month = resultSet.getString(3);
                    String day = resultSet.getString(4);

                    if ( month.length() == 1 ) {
                        month = "0" + month;
                    }
                    if ( day.length() == 1 ) {
                        day = "0" + day;
                    }
                    dateMap.put(year+month+day,id);
                }

                resultSet.close();
            } catch (Exception e){
                e.printStackTrace();
            } finally {
                if(preparedStatement != null){
                    if (resultSet != null){
                        try {
                            resultSet.close();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                    }
                    try {
                        preparedStatement.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }


        }

        @Override
        public void write(AnalysisKey text, AnalysisValue text2) throws IOException, InterruptedException {
            PreparedStatement statement = null;

            try {
                String insertSQL = "insert into ct_call(telid,dateid,sumcall,sumduration)values(?,?,?,?)";
                statement = conn.prepareStatement(insertSQL);

                statement.setInt(1,userMap.get(text.getTel()));
                statement.setInt(2,dateMap.get(text.getDate()));
                statement.setInt(3,Integer.parseInt(text2.getSumCall()));
                statement.setInt(4,Integer.parseInt(text2.getSumDuration()));
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
        }
    }

    @Override
    public RecordWriter<AnalysisKey,AnalysisValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
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