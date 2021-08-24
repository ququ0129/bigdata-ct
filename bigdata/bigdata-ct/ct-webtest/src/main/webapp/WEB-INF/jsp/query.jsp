<%@page pageEncoding="UTF-8" %>
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
<form>
    <div class="form-group">
        <label for="tel">Tel</label>
        <input type="text" class="form-control" id="tel" placeholder="please input telephone number">
    </div>
    <div class="form-group">
        <label for="calltime">Time</label>
        <input type="text" class="form-control" id="calltime" placeholder="please input call time">
    </div>
    <button type="button" class="btn btn-default" onclick="queryData()">OK</button>
</form>

<script src="/jquery/jquery-2.1.1.min.js"></script>
<script src="/bootstrap/js/bootstrap.js"></script>
<script>

    function queryData() {
        window.location.href = "/view?tel=" + $("#tel").val() + "&calltime="+$("#calltime").val();
    }
</script>
</body>
</html>