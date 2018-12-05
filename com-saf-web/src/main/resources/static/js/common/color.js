//var source_colors = ["#3fb1e3", "#6be6c1", "#626c91", "#a0a7e6", "#c4ebad", "#96dee8"];
var source_colors = ["#3fb1e3","#6be6c1","#626c91","#a0a7e6","#c4ebad","#96dee8","#444693","#b7ba6b","#426ab3","#46485f","#585eaa","#6f60aa","#543044","#7fb80e","#abc88b","#007947","#65c294","#50b7c1","#008792","#2468a2","#deab8a","#f47920","#8f4b2e","#6b473c","#f58220","#905d1d","#8c531b","#64492b","#845538","#df9464"];

var saf_color = {
    // 根据基础颜色生成临近颜色组
    random: function (num) {
        var colors = new Array();
        for (var i = 0; i < num; i++) {
            colors.push('#' + Math.floor(Math.random() * 0xffffff).toString(16));
        }
        return colors;
    },
    shade: function (startColor, endColor, step) {
        var startRGB = this.colorRgb(startColor);//转换为rgb数组模式
        var startR = startRGB[0];
        var startG = startRGB[1];
        var startB = startRGB[2];
        var endRGB = this.colorRgb(endColor);
        var endR = endRGB[0];
        var endG = endRGB[1];
        var endB = endRGB[2];
        var sR = (endR - startR) / step;//总差值
        var sG = (endG - startG) / step;
        var sB = (endB - startB) / step;
        var colorArr = [];
        for (var i = 0; i < step; i++) {
            //计算每一步的hex值
            var hex = this.colorHex('rgb(' + parseInt((sR * i + startR)) + ',' + parseInt((sG * i + startG)) + ',' + parseInt((sB * i + startB)) + ')');
            colorArr.push(hex);
        }
        return colorArr;
    },
    // 将hex表示方式转换为rgb表示方式(这里返回rgb数组模式)
    colorRgb: function (sColor) {
        var reg = /^#([0-9a-fA-f]{3}|[0-9a-fA-f]{6})$/;
        var sColor = sColor.toLowerCase();
        if (sColor && reg.test(sColor)) {
            if (sColor.length === 4) {
                var sColorNew = "#";
                for (var i = 1; i < 4; i += 1) {
                    sColorNew += sColor.slice(i, i + 1).concat(sColor.slice(i, i + 1));
                }
                sColor = sColorNew;
            }
            //处理六位的颜色值
            var sColorChange = [];
            for (var i = 1; i < 7; i += 2) {
                sColorChange.push(parseInt("0x" + sColor.slice(i, i + 2)));
            }
            return sColorChange;
        } else {
            return sColor;
        }
    },
    // 将rgb表示方式转换为hex表示方式
    colorHex: function (rgb) {
        var _this = rgb;
        var reg = /^#([0-9a-fA-f]{3}|[0-9a-fA-f]{6})$/;
        if (/^(rgb|RGB)/.test(_this)) {
            var aColor = _this.replace(/(?:(|)|rgb|RGB)*/g, "").split(",");
            var strHex = "#";
            for (var i = 0; i < aColor.length; i++) {
                var hex = Number(aColor[i]).toString(16);
                hex = hex < 10 ? 0 + '' + hex : hex;// 保证每个rgb的值为2位
                if (hex === "0") {
                    hex += hex;
                }
                strHex += hex;
            }
            if (strHex.length !== 7) {
                strHex = _this;
            }
            return strHex;
        } else if (reg.test(_this)) {
            var aNum = _this.replace(/#/, "").split("");
            if (aNum.length === 6) {
                return _this;
            } else if (aNum.length === 3) {
                var numHex = "#";
                for (var i = 0; i < aNum.length; i += 1) {
                    numHex += (aNum[i] + aNum[i]);
                }
                return numHex;
            }
        } else {
            return _this;
        }
    },
    shade_green: function (num) {
        var colors = new Array();
        var green_colors = ["00FF00", "00EE00", "00DD00", "00CC00", "00BB00", "00AA00", "009900", "008800", "007700", "006600", "005500"];
        for (var i = 0; i < num; i++) {
            if (i < green_colors.length) {
                colors.push('#' + green_colors[i]);
            } else {
                colors.push('#' + green_colors[green_colors.length]);
            }
        }
        return colors;
    },
    echarts_color: function (num) {
        var colors = new Array();
        for (var i = 0; i < num; i++) {
            if (i == source_colors.length) {
                i = 0;
            }
            colors.push(source_colors[i]);
        }
        return colors;
    }
}