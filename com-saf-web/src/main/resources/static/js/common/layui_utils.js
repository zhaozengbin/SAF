function init_layui() {
    layui.config({
        base: '/lay/modules/' //你存放新模块的目录，注意，不是layui的模块目录
    }).use('layer', 'form'); //加载入口
}

var saf_layer = {
    open: function (type, area, content, show_title, show_closeBtn, shadeClose, skin) {
        layui.use(['layer'], function (args) {
            var layer = layui.layer;
            //页面层
            layer.open({
                type: type,
                area: area, //宽高
                content: content,
                closeBtn: show_closeBtn ? 1 : 0, //不显示关闭按钮
                shadeClose: shadeClose, //开启遮罩关闭
                skin: skin,
                title: show_title //不显示标题
            });
        });
    },
    confirm: function (content, btn, fun_confirm, fun_other) {
        layui.use(['layer'], function (args) {
            var layer = layui.layer;
            layer.confirm(content, {
                btn: btn //按钮
            }, function (index, layero) {
                layer.close(index);
                try {
                    fun_confirm();
                } catch (e) {
                    console.log(e);
                }
            }, function (index, layero) {
                layer.close(index);
                try {
                    fun_other();
                } catch (e) {
                    console.log(e);
                }
            });
        });
    },
    msg: function (content) {
        layui.use(['layer'], function (args) {
            var layer = layui.layer;
            //页面层
            layer.msg(content);
        });
    },
    tips_error: function (input_name, msg) {
        if (input_name && msg) {
            $(input_name).css('border-style', 'solid').css('border-width', '1px').css('border-color', 'red');
            layer.tips(msg, input_name);
        }
    },
    tips_success: function (input_name, msg, is_tips) {
        if (input_name && msg) {
            $(input_name).css('border-style', 'solid').css('border-width', '1px').css('border-color', 'green');
            if (is_tips) {
                layer.tips(msg, input_name);
            }
        }
    }
};
var saf_layui = {
    loding_increase: function (lay_filter, schedule) {
        layui.use('element', function () {
            var element = layui.element;
            element.progress(lay_filter, schedule + '%');
        });
    }
};