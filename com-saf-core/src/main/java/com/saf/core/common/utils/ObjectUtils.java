package com.saf.core.common.utils;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.text.NumberFormat;
import java.util.*;

/**
 * 类名称：ObjectUtils <br>
 * 类描述：对象工具类 <br>
 * 创建人：赵增斌 <br>
 * 修改人：赵增斌 <br>
 * 修改时间：2017年5月4日 下午12:08:21 <br>
 * 修改备注：TODO <br>
 */
public class ObjectUtils {

    private static List<String> STRINGS = Arrays.asList(new String[]{"", "null"});

    /**
     * 方法：isEmpty <br>
     * 描述：判断集合是否为空 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-6-21 下午5:46:00 <br>
     *
     * @param collection 集合
     * @return
     */
    public static boolean isEmpty(Collection<?> collection) {
        if (collection == null || collection.size() == 0) {
            return true;
        }
        return false;
    }

    /**
     * 方法：isNotEmpty <br>
     * 描述：判断集合是否不为空 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-6-21 下午5:46:15 <br>
     *
     * @param collection 集合
     * @return
     */
    public static boolean isNotEmpty(Collection<?> collection) {
        if (collection != null && collection.size() > 0) {
            return true;
        }
        return false;
    }

    /**
     * 方法：isNotEmpty <br>
     * 描述：判断数组是否不为空 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-6-21 下午5:47:01 <br>
     *
     * @param objectArrays
     * @return
     */
    public static boolean isNotEmpty(Object[] objectArrays) {
        if (objectArrays != null && objectArrays.length > 0) {
            return true;
        }
        return false;
    }

    /**
     * 方法：isEmpty <br>
     * 描述：判断数组是否为空 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-6-21 下午5:47:09 <br>
     *
     * @param objectArrays
     * @return
     */
    public static boolean isEmpty(Object[] objectArrays) {
        if (objectArrays == null || objectArrays.length == 0) {
            return true;
        }
        return false;
    }

    /**
     * 方法：isNotEmpty <br>
     * 描述：判断对象是否不为空 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-6-21 下午5:47:01 <br>
     *
     * @param object
     * @return
     */
    public static boolean isNotEmpty(Object object) {
        if (object != null) {
            if (object instanceof String) {
                String string = (String) object;
                if (string != null && !"".equals(string) && !"null".equals(string) && string.trim().length() != 0) {
                    return true;
                }
            } else if (object instanceof Integer) {
                Integer integer = (Integer) object;
                if (integer != null) {
                    return true;
                }
            } else if (object instanceof Double) {
                Double doubles = (Double) object;
                if (doubles != null) {
                    return true;
                }
            } else if (object instanceof Float) {
                Float floats = (Float) object;
                if (floats != null) {
                    return true;
                }
            }
            if (!(object instanceof String)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 方法：isEmpty <br>
     * 描述：判断对象是否为空 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-6-21 下午5:47:09 <br>
     *
     * @param object
     * @return
     */
    public static boolean isEmpty(Object object) {
        if (object == null) {
            return true;
        }
        if (object instanceof String) {
            String string = (String) object;
            if (STRINGS.contains(string)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 方法：isEmpty <br>
     * 描述：判断字符串是否为空 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-6-21 下午5:46:51 <br>
     *
     * @param stringBuffer
     * @return
     */
    public static boolean isEmpty(StringBuffer stringBuffer) {
        if (stringBuffer == null || "".equals(stringBuffer)) {
            return true;
        }
        return false;
    }

    /**
     * 方法：isNotEmpty <br>
     * 描述：判断字符串是否不为空 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-6-21 下午5:47:01 <br>
     *
     * @param stringBuffer
     * @return
     */
    public static boolean isNotEmpty(StringBuffer stringBuffer) {
        if (stringBuffer != null && !"".equals(stringBuffer) && stringBuffer.length() > 0) {
            return true;
        }
        return false;
    }

    /**
     * 方法：isEmpty <br>
     * 描述：判断MAP是否为空 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-6-21 下午5:47:09 <br>
     *
     * @param map
     * @return
     */
    public static boolean isEmpty(Map<?, ?> map) {
        if (map == null || map.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * 方法：isNotEmpty <br>
     * 描述：判断MAP是否不为空 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-6-21 下午5:47:19 <br>
     *
     * @param map
     * @return
     */
    public static boolean isNotEmpty(Map<?, ?> map) {
        if (map != null && !map.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * 方法：isIn <br>
     * 描述：判断字符串是否在数组中包含 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-7-16 下午5:29:30 <br>
     *
     * @param substring
     * @param source
     * @return
     */
    public static boolean isIn(String substring, String[] source) {
        if (source == null || source.length == 0) {
            return false;
        }
        for (int i = 0; i < source.length; i++) {
            String aSource = source[i];
            if (aSource.equals(substring)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 方法：isIn <br>
     * 描述：判断字符串是否在数组中包含 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-7-16 下午5:29:30 <br>
     *
     * @param substring
     * @param collection
     * @return
     */
    public static boolean isIn(String substring, Collection<String> collection) {
        if (collection == null || collection.size() == 0) {
            return false;
        }
        Iterator<String> iterator = collection.iterator();
        while (iterator.hasNext()) {
            String aSource = iterator.next();
            if (aSource.equals(substring)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 方法：decode <br>
     * 描述：解码 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-10-9 下午6:44:08 <br>
     *
     * @param strIn
     * @param sourceCode
     * @param targetCode
     * @return
     */
    public static String decode(String strIn, String sourceCode, String targetCode) {
        String temp = code2code(strIn, sourceCode, targetCode);
        return temp;
    }

    /**
     * 方法：StrToUTF <br>
     * 描述： 字符串转编码转换 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-10-9 下午6:44:20 <br>
     *
     * @param strIn
     * @param sourceCode
     * @param targetCode
     * @return
     */
    public static String StrToUTF(String strIn, String sourceCode, String targetCode) {
        strIn = "";
        try {
            strIn = new String(strIn.getBytes("ISO-8859-1"), "GBK");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return strIn;

    }

    /**
     * 方法：code2code <br>
     * 描述：TODO <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-10-9 下午6:44:29 <br>
     *
     * @param strIn
     * @param sourceCode
     * @param targetCode
     * @return
     */
    private static String code2code(String strIn, String sourceCode, String targetCode) {
        String strOut = null;
        if (strIn == null || (strIn.trim()).equals(""))
            return strIn;
        try {
            byte[] b = strIn.getBytes(sourceCode);
            strOut = new String(b, targetCode);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return strOut;
    }

    /**
     * 方法：getIntegerArry <br>
     * 描述：获取整型数组 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-10-9 下午6:44:37 <br>
     *
     * @param object
     * @return
     */
    public static Integer[] getIntegerArry(String[] object) {
        int len = object.length;
        Integer[] result = new Integer[len];
        try {
            for (int i = 0; i < len; i++) {
                result[i] = new Integer(object[i].trim());
            }
            return result;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 方法：getIntegerArry <br>
     * 描述：获取整型数组 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2013-10-9 下午6:44:37 <br>
     *
     * @param object
     * @return
     */
    public static Long[] getLongArry(String[] object) {
        int len = object.length;
        Long[] result = new Long[len];
        try {
            for (int i = 0; i < len; i++) {
                result[i] = new Long(object[i].trim());
            }
            return result;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 方法：convertToCode <br>
     * 描述： 使用commons的jexl可实现将字符串变成可执行代码的功能 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2017年3月20日 上午11:24:41 <br>
     *
     * @param jexlExp
     * @param map
     * @return
     */
    public static Object convertToCode(String jexlExp, Map<String, Object> map) {
        JexlEngine jexl = new JexlEngine();
        Expression e = jexl.createExpression(jexlExp);
        JexlContext jc = new MapContext();
        for (String key : map.keySet()) {
            jc.set(key, map.get(key));
        }
        if (null == e.evaluate(jc)) {
            return "";
        }
        return e.evaluate(jc);
    }

    /**
     * 方法：random <br>
     * 描述：获取随机数 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2017年5月17日 上午10:31:42 <br>
     *
     * @param min
     * @param max
     * @return
     */
    public static int random(int min, int max) {
        Random random = new Random();
        int s = random.nextInt(max) % (max - min + 1) + min;
        return s;
    }

    /**
     * 方法：numFormat <br>
     * 描述：数字格式化 <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2017年8月31日 上午11:29:19 <br>
     *
     * @param maximumIntegerDigits  最大整数位数
     * @param minimumIntegerDigits  最小整数位数
     * @param maximumFractionDigits 最大小数位数
     * @param minimumFractionDigits 最小小数位数
     * @param num                   格式化的数字
     * @return
     */
    public static String numFormat(Integer maximumIntegerDigits, Integer minimumIntegerDigits,
                                   Integer maximumFractionDigits, Integer minimumFractionDigits, long num) {
        // 得到一个NumberFormat的实例
        NumberFormat nf = NumberFormat.getInstance();
        // 设置是否使用分组
        nf.setGroupingUsed(false);
        if (isNotEmpty(maximumIntegerDigits)) {
            // 设置最大整数位数
            nf.setMaximumIntegerDigits(maximumIntegerDigits);
        }
        if (isNotEmpty(minimumIntegerDigits)) {
            // 设置最小整数位数
            nf.setMinimumIntegerDigits(minimumIntegerDigits);
        }
        if (isNotEmpty(maximumFractionDigits)) {
            // 设置最大小数位数
            nf.setMaximumFractionDigits(maximumFractionDigits);
        }
        if (isNotEmpty(minimumFractionDigits)) {
            // 设置最大小数位数
            nf.setMinimumFractionDigits(minimumFractionDigits);
        }
        // 输出测试语句
        return nf.format(num);
    }

    public static void requireNonNull(Object... obj) {
        if (obj.length % 2 != 0) {
            throw new IllegalArgumentException("arguments's length mod 2 must be 0.");
        }

        for (int i = 0; i < obj.length; i += 2) {
            if (obj[i] == null) {
                throw new NullPointerException(obj[i + 1].toString());
            }
        }
    }

    public static <T> T requiredNonNull(T object) {
        if (object == null) {
            throw new NullPointerException();
        }
        return object;
    }

    public static boolean paramsIsEmpty(Object... param) {
        if (param == null || param.length == 0)
            return true;
        for (int i = 0; i < param.length; i++) {
            if (param[i] instanceof String) {
                return StringUtils.isBlank((String) param[i]);
            } else if (param[i] == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * 方法：search <br>
     * 描述：TODO <br>
     * 作者：赵增斌 E-mail:zhaozengbin@gmail.com QQ:4415599
     * weibo:http://weibo.com/zhaozengbin <br>
     * 日期： 2017年11月28日 下午3:35:55 <br>
     *
     * @param patternStr
     * @param set
     * @return
     */
    public static Set<String> search(String patternStr, Set<String> set) {
        Set<String> results = new HashSet<String>();
        for (String result : set) {
            if (result.toLowerCase().indexOf(patternStr.toLowerCase()) >= 0) {
                results.add(result);
            }
        }
        return results;
    }

    public static <T> List<T> object2basic(Collection list, Class<T> clazz) {
        if (isNotEmpty(list)) {
            List<T> result = new ArrayList<>();
            for (Object item : list) {
                if (clazz.isInstance(item)) {
                    result.add((T) item);
                }
            }
            return result;
        }
        return null;
    }

    public static List<Integer> string2IntegerList(String str, String delimiter) {
        if (isNotEmpty(str)) {
            List<Integer> result = new ArrayList<>();
            if (str.contains(delimiter)) {
                String[] items = str.split(delimiter);
                for (String item : items) {
                    result.add(Integer.parseInt(item));
                }
            } else {
                result.add(Integer.parseInt(str));
            }
            return result;
        }
        return null;
    }

    public static List<Double> string2DoubleList(String str, String delimiter) {
        if (isNotEmpty(str)) {
            List<Double> result = new ArrayList<>();
            if (str.contains(delimiter)) {
                String[] items = str.split(delimiter);
                for (String item : items) {
                    result.add(Double.parseDouble(item));
                }
            } else {
                result.add(Double.parseDouble(str));
            }
            return result;
        }
        return null;
    }

    public static boolean isNumber(String string) {
        return StringUtils.isNumeric(string);
    }

    public static long stringToNumber(String string) {
        return string.hashCode() % 100;
    }

    public static void main(String[] args) {
        System.out.println(numFormat(10, 10, 0, 0, 10));
    }
}
