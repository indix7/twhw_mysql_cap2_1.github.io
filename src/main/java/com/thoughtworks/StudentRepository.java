package com.thoughtworks;

import javax.xml.transform.Result;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class StudentRepository {
    private Connection connection = null;
    private Statement statement = null;
    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    public StudentRepository() {
        try {
            this.connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/thoughtworks" +
                            "?useUnicode=true&characterEncoding=utf-8&serverTimezone=Hongkong",
                    "root", "mysql");
            this.statement = this.connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @ Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.statement.close();
        this.connection.close();
    }

    public void save(List<Student> students) {
        String sql = "SHOW TABLES LIKE 'student'";
        try {
            if (statement.executeUpdate(sql) > -1) {
                String sqlCreate = "CREATE TABLE student (" +
                        "id VARCHAR(10) NOT NULL," +
                        "name VARCHAR(50) NOT NULL," +
                        "gender ENUM('男', '女') NOT NULL," +
                        "admission YEAR NOT NULL," +
                        "birthday DATE NOT NULL," +
                        "class VARCHAR(10) NOT NULL," +
                        "PRIMARY KEY(id)"+
                        ")ENGINE=InnoDB CHARSET=utf8mb4";
                statement.executeUpdate(sqlCreate);
                students.forEach(this::save);
            } else {
              
                ResultSet resultSet =  statement.executeQuery("SELECT id from student");
                List<String> existId = new LinkedList<>();
                while (resultSet.next()) {
                    existId.add(resultSet.getString("id"));
                }
                for(Student student :students) {
                    if (existId.contains(student.getId())) {
                        this.update(student.getId(), student);
                    } else {
                        this.save(student);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void save(Student student) {
        // TODO:
        try {
            String sql = String.format(
                    "INSERT INTO student " +
                            "(id, name, gender, admission, birthday, class) " +
                            "VALUES " +
                            "('%s', '%s', '%s', '%s', '%s', '%s')",
                    student.getId(),
                    student.getName(),
                    student.getGender(),
                    student.getAdmissionYear(),
                    format.format(student.getBirthday()),
                    student.getClassId());
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<Student> query() {
        // TODO:
        String sql = "SELECT id, name, gender, admission, birthday, class FROM student";
        List<Student> list = new LinkedList<>();
        try {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                list.add(new Student(resultSet.getString("id"),
                        resultSet.getString("name"),
                        resultSet.getString("gender"),
                        resultSet.getInt("admission"),
                        resultSet.getString("birthday"),
                        resultSet.getString("class")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public List<Student> queryByClassId(String classId) {
        // TODO:
        String sql = String.format(
                "SELECT " +
                        "id, name, gender, admission, birthday, class " +
                        "FROM student " +
                        "WHERE class = '%s' " +
                        "ORDER BY id DESC", classId);
        List<Student> list = new LinkedList<>();
        try {
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                list.add(new Student(resultSet.getString("id"),
                        resultSet.getString("name"),
                        resultSet.getString("gender"),
                        resultSet.getInt("admission"),
                        resultSet.getString("birthday"),
                        resultSet.getString("class")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public void update(String id, Student student) {
        // TODO:
        String sql = String.format(
                "UPDATE student " +
                        "SET id = '%s', name= '%s', gender = '%s', admission = '%s', birthday = '%s', class = '%s'" +
                        "WHERE id = '%s'",
                student.getId(),
                student.getName(),
                student.getGender(),
                student.getAdmissionYear(),
                format.format(student.getBirthday()),
                student.getClassId(),
                id);
        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void delete(String id) {
        // TODO:
        String sql = String.format("DELETE FROM student WHERE id = %s", id);
        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
