var spark_setting_form_id = "#spark_setting_form";
var data_setting_form_id = "#data_setting_form";
var monitor_path = $('#monitor_path').val();

var stompClient = null;
var current_submissionid;

// 初始化加载
(function ($) {
    init_step3_form();
    init_step4_form();
    disconnect_step3(true);
    disconnect_step4(true);
    collapse_step3_init();
    collapse_step4_init();
})(jQuery);

// spark数据检测按钮绑定事件
$('#button_data_setting_back').bind('click', function () {
    if (check_hdfs_files()) {
        addSession(data_setting_form_id, function (data) {
            $('#spark_setting_tab').click()
        });
    }
});
// spark数据保存按钮绑定事件
$('#button_data_setting_next').bind('click', function () {
    if (check_hdfs_files()) {
        addSession(data_setting_form_id, function (data) {
            $('#training_setting_tab').click()
        });
    }
});

// spark数据检测按钮绑定事件
$('#button_data_setting_check').bind('click', function () {
    check_hdfs_files();
});
