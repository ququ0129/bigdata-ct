package org.cg.ct.analysis;

import org.apache.hadoop.util.ToolRunner;
import org.cg.ct.analysis.tool.AnalysisBeanTool;
import org.cg.ct.analysis.tool.AnalysisTextTool;

public class AnalysisData {
    public static void main(String[] args) throws Exception{
        //int result = ToolRunner.run( new AnalysisTextTool(),args);
        int result = ToolRunner.run( new AnalysisBeanTool(),args);
    }
}
