package com.saf.mllib.als.app.entity;

import com.saf.mllib.als.app.AbstractALS;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import scala.Serializable;
import scala.Tuple4;

public class ALSUsers extends AbstractALS<ALSUsers.Users> implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(ALSUsers.class);

    public ALSUsers() {
    }

    public ALSUsers(String dataFilePath, JavaSparkContext javaSparkContex) {
        super(dataFilePath, javaSparkContex);
    }

    @Override
    public JavaRDD createJavaRDD() {
        JavaRDD<String> data = super.dataFileToJavaRDD();
        // 将电影的id，标题，类型以三元组的形式保存
        JavaRDD<Tuple4<Integer, String, Integer, Integer>> javaRDD = data.map(new Function<String, Tuple4<Integer, String, Integer, Integer>>() {
            @Override
            public Tuple4<Integer, String, Integer, Integer> call(String line) {
                String[] fields = line.split("::");
                if (fields.length != 5) {
                    throw new IllegalArgumentException("Each line must contain 3 fields");
                }
                int id = Integer.parseInt(fields[0]);
                String sex = fields[1];
                int age = Integer.parseInt(fields[2]);
                int occupation = Integer.parseInt(fields[3]);
                LOGGER.debug(String.format("ALSUsers JavaRDD : userId ==> %d , sex ==> %s , age ==> %d , occupation ==> %d", id, sex, age, occupation));
                Tuple4<Integer, String, Integer, Integer> tuple4 = new Tuple4<>(id, sex, age, occupation);
                return tuple4;
            }
        });
        return javaRDD;
    }

    @Override
    public Dataset<Users> javaRDDToDataSet(JavaRDD javaRDD) {
        // 将电影的id，年龄，职业以三元组的形式保存
        JavaRDD<Users> userJavaRDD = javaRDD.map(new Function<Tuple4<Integer, String, Integer, Integer>, Users>() {
            @Override
            public Users call(Tuple4<Integer, String, Integer, Integer> tuple4) throws Exception {
                if (tuple4 == null) {
                    throw new IllegalArgumentException("Each line must contain 5 fields");
                }
                int id = tuple4._1();
                String sex = tuple4._2();
                int age = tuple4._3();
                int occupation = tuple4._4();
                LOGGER.debug(String.format("ALSUsers JavaRDDToDataSet : userId ==> %d , sex ==> %s , age ==> %d , occupation ==> %d", id, sex, age, occupation));
                Users user = new Users(id, age, occupation);
                return user;
            }
        });
        SQLContext sqlContext = new SQLContext(super.getJavaSparkContex());
        Dataset result = sqlContext.createDataFrame(userJavaRDD, Users.class);
        return result;
    }

    public static class Users implements Serializable{
        private int id;

        private String sex;

        private int age;

        private int occupation;

        public Users(int id, int age, int occupation) {
            this.id = id;
            this.age = age;
            this.occupation = occupation;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public int getOccupation() {
            return occupation;
        }

        public void setOccupation(int occupation) {
            this.occupation = occupation;
        }
    }
}
