<%@page pageEncoding="UTF-8" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Bootstrap 101 Template</title>
    <link href="/bootstrap/css/bootstrap.css" rel="stylesheet">
</head>
<body>

    <div id="main" style="width: 800px;height:600px;"></div>

    <script src="/jquery/jquery-2.1.1.min.js"></script>
    <script src="/bootstrap/js/bootstrap.js"></script>
    <script src="/jquery/echarts.min.js"></script>
    <script>

        var myChart = echarts.init(document.getElementById('main'));

        option = {
            title : {
                text: 'Subscriber Call Statistics',
            },
            tooltip : {
                trigger: 'axis'
            },
            legend: {
                data:['Number of calls','Duration of calls']
            },
            toolbox: {
                show : true,
                feature : {
                    dataView : {show: true, readOnly: false},
                    magicType : {show: true, type: ['line', 'bar']},
                    restore : {show: true},
                    saveAsImage : {show: true}
                }
            },
            calculable : true,
            xAxis : [
                {
                    type : 'category',
                    data : [
                        <c:forEach items="${calllogs}" var="calllog" >
                        ${calllog.dateid},
                        </c:forEach>
                    ]
                }
            ],
            yAxis : [
                {
                    type : 'value'
                }
            ],
            series : [
                {
                    name:'Call Times',
                    type:'bar',
                    data:[
                        <c:forEach items="${calllogs}" var="calllog" >
                        ${calllog.sumcall},
                        </c:forEach>
                    ],
                    markPoint : {
                        data : [
                            {type : 'max', name: 'MAX'},
                            {type : 'min', name: 'MIN'}
                        ]
                    },
                    markLine : {
                        data : [
                            {type : 'average', name: 'AVERAGE'}
                        ]
                    }
                },
                {
                    name:'Call Duration',
                    type:'bar',
                    data:[
                        <c:forEach items="${calllogs}" var="calllog" >
                        ${calllog.sumduration},
                        </c:forEach>
                    ],
                    markPoint : {
                        data : [
                            {name : 'Annual High', value : 182.2, xAxis: 7, yAxis: 183},
                            {name : 'Annual Low', value : 2.3, xAxis: 11, yAxis: 3}
                        ]
                    },
                    markLine : {
                        data : [
                            {type : 'average', name : 'AVERAGE'}
                        ]
                    }
                }
            ]
        };

        myChart.setOption(option);
    </script>
</body>
</html>