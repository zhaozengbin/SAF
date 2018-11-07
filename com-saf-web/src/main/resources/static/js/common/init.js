(function ($) {
    init_layui();
    init_login_status();
})(jQuery);


function init_login_status() {
    var url = $('#security_path').val() + '/security/checkUser';
    http_utils.postData(url, null, false, function (result) {
        if (result.status && result.data) {
            $('#user_name').text(result.data);
        } else {
            window.location.href = "login.html";
        }
    });
}

$('#a_logout').bind('click', function () {
    var url = $("#security_path").val() + '/security/logout';
    http_utils.postData(url, null, false, function (result) {
        if (result.status) {
            saf_layer.msg("登出成功!");
            window.location.href = "login.html";
        }
    });
});
