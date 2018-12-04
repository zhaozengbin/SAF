function init_step3_form() {
    layui.use('form', function () {
        var form = layui.form;
        var form_id = '#form_variance_setting';
        $('#button_variance_setting').bind('click', function () {
            connect_step3();
            reload_step3_ui(true);
            $('#variance_model_tbody').children('tr').remove();
            http_utils.postData(monitor_path + '/mllib/kmeans/execute', JSON.stringify($(form_id).serializeJSON()), true, function (data) {
                if (data) {
                    if (data.status == 200) {
                        saf_layui.loding_increase('preview_kmeans', 2);
                    } else if (data.status == 1000) {
                        layer.alert(JSON.parse(data.data).exception);
                    } else if (data.data && data.data.indexOf("{") >= 0 && data.data.indexOf("}") > 0) {
                        layer.alert(JSON.parse(data.data).exception);
                    } else {
                        layer.alert(data.data);
                    }
                    if (data.data && data.data.indexOf("{") >= 0 && data.data.indexOf("}") > 0) {
                        kmeans_current_submissionid = JSON.parse(data.data).KMEANS_CURRENT_SUBMISSIONID;
                    }
                } else {
                    layer.alert('提交失败');
                }
            });
        });
        //监听指定开关
        form.on('switch(local_model_input)', function (data) {
            $('#local_model').attr('value', this.checked);
        });
    });
}

function collapse_step3_init() {
    layui.use(['element', 'layer'], function () {
        var element = layui.element;
        var layer = layui.layer;
        //监听折叠
        element.on('collapse(variance_model_collapse)', function (data) {
            // layer.msg('展开状态：' + data.show);
        });
        element.on('collapse(test_model_collapse)', function (data) {
            // layer.msg('展开状态：' + data.show);
        });
    });
}

function reload_step3_ui(is_reload) {
    if (is_reload) {
        saf_layui.loding_increase('preview_step3_kmeans', 1);
    } else {
        saf_layui.loding_increase('preview_step3_kmeans', 0);
    }
}

function connect_step3() {
    var socket = new SockJS(monitor_path + '/endpointWisely'); //1连接SockJS的endpoint是“endpointWisely”，与后台代码中注册的endpoint要一样。
    stompClient = Stomp.over(socket);//2创建STOMP协议的webSocket客户端。
    stompClient.connect({}, function (frame) {//3连接webSocket的服务端。
        reload_step3_ui(true);
        console.log('开始进行连接Connected: ' + frame);
        //4通过stompClient.subscribe（）订阅服务器的目标是'/topic/getResponse'发送过来的地址，与@SendTo中的地址对应。
        stompClient.subscribe('/topic/getResponse', function (respnose) {
            showResponse_step3(JSON.parse(respnose.body));
        });
    });
}

function disconnect_step3(is_reload) {
    if (stompClient != null) {
        stompClient.disconnect();
    }
    if (is_reload) {
        reload_step3_ui(false);
    }
    console.log("Disconnected");
}

function showResponse_step3(message) {
    if (message.responseMessageType == 'progress') {
        saf_layui.loding_increase('preview_step3_kmeans', message.responseMessage);
    } else if (message.responseMessageType == 'console'
        && message.responseMessageId.indexOf('variance') >= 0) {
        if (message.responseMessageFormat == 'json') {
            var json = JSON.parse(message.responseMessage);
            if (json.type == 1) {
                if (json.data) {
                    var data = json.data;
                    // var msg = '<p style="{0}">元素因素:{1},正则系数:{2},信心权重:{3},循环次数:{4} --- 评分:{5}</p>';
                    var rank_min_max = '(' + $('#kmeans_rank_min').val() + '/' + $('#kmeans_rank_max').val() + ')';
                    var lambda_min_max = '(' + $('#kmeans_lambda_min').val() + '/' + $('#kmeans_lambda_max').val() + ')';
                    var alpha_min_max = '(' + $('#kmeans_alpha_min').val() + '/' + $('#kmeans_alpha_max').val() + ')';
                    var iter_min_max = '(' + $('#kmeans_iter_min').val() + '/' + $('#kmeans_iter_max').val() + ')';


                    var msg = '<tr style="{0}"><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td>{5}</td><td>{6}</td></tr>';
                    var style = '';
                    var is_end = fkmeanse;
                    var data_type = '验证数据';
                    if (data.isBest) {
                        style = 'color: red;font-weight: 900;';
                        $('#kmeans_best_rank').attr('value', data.rank);
                        $('#kmeans_best_lambda').attr('value', data.lambda);
                        $('#kmeans_best_alpha').attr('value', data.alpha);
                        $('#kmeans_best_iter').attr('value', data.iter);
                        data_type = '测试数据<br >(通过验证数据获取的最优计算模型)';
                        is_end = true;
                    }
                    $('#variance_model_tbody').append(msg.format(style, rank_min_max + ' - ' + data.rank, lambda_min_max + ' - ' + data.lambda, alpha_min_max + ' - ' + data.alpha, iter_min_max + ' - ' + data.iter, data.dataRnse, data_type));
                    if (is_end) {
                        saf_layer.confirm('根据提供的算法参数已经计算出最优方案,是否进行数据推荐计算', ['是', '否'], function () {
                                $('#recommend_setting_tab').click();
                            }
                        )
                    }
                }
            }
        } else if (message.responseMessageFormat == 'string') {
            $('#spark_variance_run_status').html(message.responseMessage);
        }
        if (message == '100') {
            disconnect_step3(false);
        }
    }
}