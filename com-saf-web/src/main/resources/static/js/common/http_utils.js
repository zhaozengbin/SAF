/**
 * ajax请求封装
 * @param url 请求地址
 * @param params 请求参数
 * @param async 是否异步, 默认值为：true.true：异步，false：同步
 * @param callback 执行成功后的回调方法
 */
var http_utils = {
    getData: function (url, params, async, callback, errorMethod) {
        if (params == null || params == "") {
            params = {"t": new Date().getTime()};
        }
        if (async == null) {
            async = true;
        }
        $.ajax({
            url: url,
            data: params,
            type: "GET",
            dataType: 'json',
            cache: false,
            async: async,
            crossDomain: true,
            xhrFields: {withCredentials: true},
            success: function (result) {
                if (typeof callback == 'function') {
                    callback(result);
                }
            },
            complete: function (xhr, ts) {
            },
            error: function (XMLHttpRequest, textStatus, errorThrown) {
                if (window.console) {
                    console.log("status:" + XMLHttpRequest.status);
                    console.log("readyState:" + XMLHttpRequest.readyState);
                    console.log("textStatus:" + textStatus);
                }
                if (errorMethod && typeof errorMethod == "function") {
                    errorMethod(XMLHttpRequest, textStatus, errorThrown);
                }
            }
        })
    },

    /***
     * ajax请求封装
     * @param url 请求地址
     * @param params 请求参数
     * @param async 是否异步,默认值为：true.<br/>  true：异步，false：同步
     * @param callback 执行成功后的回调方法
     */
    postData: function (url, params, async, callback, errorMethod) {
        if (params == null || params == "") {
            params = {"t": new Date().getTime()};
        }
        if (async == null) {
            async = true;
        }
        $.ajax({
            url: url,
            data: params,
            type: 'POST',
            contentType: 'application/json',
            dataType: 'json',
            cache: false,
            async: async,
            crossDomain: true,
            xhrFields: {withCredentials: true},
            success: function (result) {
                if (typeof callback == 'function') {
                    callback(result);
                }
            },
            complete: function (xhr, ts) {

            },
            error: function (XMLHttpRequest, textStatus, errorThrown) {
                if (window.console) {
                    console.log("status:" + XMLHttpRequest.status);
                    console.log("readyState:" + XMLHttpRequest.readyState);
                    console.log("textStatus:" + textStatus);
                }
                if (errorMethod && typeof errorMethod == "function") {
                    errorMethod(XMLHttpRequest, textStatus, errorThrown);
                }
            }
        })
    },
    /***
     * ajax请求封装
     * @param formId 表单的ID
     * @param async 是否异步,默认值为：true.<br/>  true：异步，false：同步
     * @param callback 执行成功后的回调方法
     * @param errorMethod 出错时的回调方法
     */
    postFormData: function (formId, async, callback, errorMethod) {
        var j_searchForm = $("#" + formId);
        var url = j_searchForm.attr("action");
        var params = j_searchForm.serialize();
        if (params == null || params == "") {
            params = {"t": new Date().getTime()};
        }
        if (async == null) {
            async = true;
        }
        $.ajax({
            url: url,
            data: params,
            type: "POST",
            dataType: 'json',
            cache: false,
            async: async,
            crossDomain: true,
            xhrFields: {withCredentials: true},
            success: function (result) {
                if (typeof callback == 'function') {
                    var isPrompt = errorMessage(result);
                    if (!isPrompt) {
                        callback(result);
                    }
                }
            },
            complete: function (xhr, ts) {

            },
            error: function (XMLHttpRequest, textStatus, errorThrown) {
                if (window.console) {
                    console.log("status:" + XMLHttpRequest.status);
                    console.log("readyState:" + XMLHttpRequest.readyState);
                    console.log("textStatus:" + textStatus);
                }
                if (errorMethod && typeof errorMethod == "function") {
                    errorMethod(XMLHttpRequest, textStatus, errorThrown);
                }
            }
        })
    },
    getElements: function (formId, elementTypes) {
        var form = document.getElementById(formId);
        var elements = new Array();
        if (!elementTypes) {
            elementTypes = new Array();
            elementTypes.push('input');
        }
        for (var i = 0; i < elementTypes.length; i++) {
            var tagElements = form.getElementsByTagName(elementTypes[i]);
            for (var j = 0; j < tagElements.length; j++) {
                elements.push(tagElements[j]);
            }
        }
        return elements;
    }
}
