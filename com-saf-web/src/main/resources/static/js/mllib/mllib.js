// 检测配置并提示
function check(input_id, form_id, check_url, submit_input_id) {
    var flag = false;
    var form_data = $(form_id).serializeJSON();
    if (submit_input_id) {
        for (var key in form_data) {
            if (submit_input_id.indexOf(key) >= 0) {
                continue;
            } else {
                delete form_data[key];
            }
        }
    }
    http_utils.postData(monitor_path + check_url, JSON.stringify(form_data), false, function (result) {
        if (result && result.status == 200) {
            flag = true;
            saf_layer.tips_success(input_id, result.data);
        } else {
            flag = false;
            saf_layer.tips_error(input_id, result.data);
        }
    });
    return flag;
}

function check_hdfs() {
    return check('#hdfs_path', spark_setting_form_id, '/hdfs/checkHDFSStatus');
}

function check_hdfs_tmp() {
    return check('#check_point_dir', spark_setting_form_id, '/hdfs/checkHDFSTmpStatus');
}

function check_hdfs_file(div_id) {
    return check('#' + div_id, data_setting_form_id, '/hdfs/checkHDFSFileStatus', div_id);
}

// 遍历检测所有数据文件
function check_hdfs_files() {
    var file_paths = $(data_setting_form_id).serializeJSON();
    var flag = true;
    for (var key in file_paths) {//遍历json对象的每个key/value对,p为key
        flag = check_hdfs_file(key);
        if (!flag) {
            return flag;
        }
    }
    return flag;
}

//添加session
function addSession(formid, fun) {
    var url = $('#security_path').val() + '/sessionCache/add';    //设置新提交地址
    http_utils.postData(url, JSON.stringify($(formid).serializeJSON()), false, function (result) {
        if (result && result.status == 200) {
            saf_layer.msg(result.data);
            fun(result);
        }
    });
}

// 保存spark hadoop配置并继续操作
$('#button_spark_setting_next').bind('click', function () {
    if (check_hdfs()) {
        if (check_hdfs_tmp()) {
            addSession(spark_setting_form_id, function (data) {
                $('#data_setting_tab').click()
            });
        }
    }
    return false;
});

// hadoop检测按钮绑定事件
$('#button_spark_setting_check').bind('click', function () {
    if (check_hdfs()) {
        check_hdfs_tmp();
    }
});