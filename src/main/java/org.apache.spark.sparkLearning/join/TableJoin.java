package org.apache.spark.sparkLearning.join;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sparkLearning.bean.Course;
import org.apache.spark.sparkLearning.bean.Student;
import org.apache.spark.sparkLearning.bean.StudentCourse;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jsen.yin [jsen.yin@gmail.com]
 * 2019-01-26
 * @Description: <p></p>
 */
public class TableJoin {

    public static void main(String[] args) {


        List<Student> students = new ArrayList<>();
        students.add(JSONObject.parseObject("{\"sid\":\"S001\",\"sanme\":\"zhangsan\",\"age\":\"12\",\"gender\":\"female\"}", Student.class));
        students.add(JSONObject.parseObject("{\"sid\":\"S002\",\"sanme\":\"lisi\",\"age\":\"13\",\"gender\":\"male\"}", Student.class));
        students.add(JSONObject.parseObject("{\"sid\":\"S003\",\"sanme\":\"wangwu\",\"age\":\"14\",\"gender\":\"male\"}", Student.class));
        students.add(JSONObject.parseObject("{\"sid\":\"S004\",\"sanme\":\"zhaoliu\",\"age\":\"15\",\"gender\":\"female\"}", Student.class));


        List<Course> courses = new ArrayList<>();
        courses.add(JSONObject.parseObject("{\"cid\":\"C001\",\"cname\":\"football\"}", Course.class));
        courses.add(JSONObject.parseObject("{\"cid\":\"C002\",\"cname\":\"music\"}", Course.class));
        courses.add(JSONObject.parseObject("{\"cid\":\"C003\",\"cname\":\"art\"}", Course.class));

        List<StudentCourse> studentCourses = new ArrayList<>();
        studentCourses.add(JSONObject.parseObject("{\"id\":\"1\",\"sid\":\"S001\",\"cid\":\"C001\"}",StudentCourse.class));
        studentCourses.add(JSONObject.parseObject("\"id\":\"2\",\"sid\":\"S002\",\"cid\":\"C001\"}",StudentCourse.class));
        studentCourses.add(JSONObject.parseObject("\"id\":\"3\",\"sid\":\"S002\",\"cid\":\"C002\"}",StudentCourse.class));
        studentCourses.add(JSONObject.parseObject("\"id\":\"4\",\"sid\":\"S003\",\"cid\":\"C003\"}",StudentCourse.class));
        studentCourses.add(JSONObject.parseObject("\"id\":\"5\",\"sid\":\"S003\",\"cid\":\"C001\"}",StudentCourse.class));
        studentCourses.add(JSONObject.parseObject("\"id\":\"6\",\"sid\":\"S004\",\"cid\":\"C003\"}",StudentCourse.class));
        studentCourses.add(JSONObject.parseObject("\"id\":\"7\",\"sid\":\"S004\",\"cid\":\"C002\"}",StudentCourse.class));





    }


}
