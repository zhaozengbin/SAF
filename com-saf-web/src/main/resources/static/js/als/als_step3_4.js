function init_step4_form() {
    layui.use('form', function () {
        var form = layui.form;
        var form_id = '#form_best_recommend_setting';
        $('#button_recommend_setting').bind('click', function () {
            connect_step4();
            reload_step4_ui(true);
            $('#recommend_model_tbody').children('tr').remove();
            var formData = $(form_id).serializeJSON();
            formData.als_training_proportion = $('#als_training_proportion').val();
            formData.als_validata_proportion = $('#als_validata_proportion').val();
            formData.als_test_proportion = $('#als_test_proportion').val();
            http_utils.postData(monitor_path + '/mllib/als/execute', JSON.stringify(formData), true, function (data) {
                if (data) {
                    if (data.status == 200) {
                        saf_layui.loding_increase('preview_als', 2);
                    } else if (data.data && data.data.indexOf("{") >= 0 && data.data.indexOf("}") > 0) {
                        layer.alert(JSON.parse(data.data).exception);
                    } else {
                        layer.alert(data.data);
                    }
                    if (data.data && data.data && data.data.indexOf("{") >= 0 && data.data.indexOf("}") > 0) {
                        als_current_submissionid = JSON.parse(data.data).ALS_CURRENT_SUBMISSIONID;
                    }
                } else {
                    layer.alert('提交失败');
                }
            });
        });
        //监听指定开关
        form.on('switch(recommend_local_model_input)', function (data) {
            $('#recommend_local_model').attr('value', this.checked);
        });
    });
}

function collapse_step4_init() {
    layui.use(['element', 'layer'], function () {
        var element = layui.element;
        var layer = layui.layer;
        //监听折叠
        element.on('collapse(recommend_model_collapse)', function (data) {
            // layer.msg('展开状态：' + data.show);
        });
    });
}

function reload_step4_ui(is_reload) {
    if (is_reload) {
        saf_layui.loding_increase('preview_step4_als', 1);
    } else {
        saf_layui.loding_increase('preview_step4_als', 0);
    }
}

function connect_step4() {
    var socket = new SockJS(monitor_path + '/endpointWisely'); //1连接SockJS的endpoint是“endpointWisely”，与后台代码中注册的endpoint要一样。
    stompClient = Stomp.over(socket);//2创建STOMP协议的webSocket客户端。
    stompClient.connect({}, function (frame) {//3连接webSocket的服务端。
        reload_step4_ui(true);
        console.log('开始进行连接Connected: ' + frame);
        //4通过stompClient.subscribe（）订阅服务器的目标是'/topic/getResponse'发送过来的地址，与@SendTo中的地址对应。
        stompClient.subscribe('/topic/getResponse', function (respnose) {
            showResponse_step4(JSON.parse(respnose.body));
        });
    });
}

function disconnect_step4(is_reload) {
    if (stompClient != null) {
        stompClient.disconnect();
    }
    if (is_reload) {
        reload_step4_ui(false);
    }
    console.log("Disconnected");
}

function showResponse_step4(message) {
    if (message.responseMessageType == 'progress') {
        saf_layui.loding_increase('preview_step4_als', message.responseMessage);
    } else if (message.responseMessageType == 'console'
        && message.responseMessageId.indexOf('variance') >= 0) {
        if (message.responseMessageFormat == 'json') {
            var json = JSON.parse(message.responseMessage);
            if (json.type == 2) {
                if (json.data) {
                    var data = json.data;
                    var msg = '<tr style="{0}"><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td></tr>';
                    var style = '';
                    $('#recommend_model_tbody').append(msg.format(style, data.id, data.title, data.type, data.rating));
                }
            }
        } else if (message.responseMessageFormat == 'string') {
            $('#spark_recommend_run_status').html(message.responseMessage);
        }
        if (message == '100') {
            disconnect_step4(false);
        }
    }
}