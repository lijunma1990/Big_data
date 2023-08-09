package org.example.pojo;



import java.io.Serializable;
import java.util.Objects;

public class Student implements Serializable {
    private static final long serialVersionUID = -2789105097448974920L;
    private Integer id;
    private String stuNumbers;
    private String name;
    private int age;
    private String classNum;

    public Student() {
    }

    public Student(Integer id, String stuNumbers, String name, int age, String classNum) {
        this.id = id;
        this.stuNumbers = stuNumbers;
        this.name = name;
        this.age = age;
        this.classNum = classNum;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getStuNumbers() {
        return stuNumbers;
    }

    public void setStuNumbers(String stuNumbers) {
        this.stuNumbers = stuNumbers;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getClassNum() {
        return classNum;
    }

    public void setClassNum(String classNum) {
        this.classNum = classNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Student)) return false;
        Student student = (Student) o;
        return age == student.age && Objects.equals(id, student.id) && Objects.equals(stuNumbers, student.stuNumbers) && Objects.equals(name, student.name) && Objects.equals(classNum, student.classNum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, stuNumbers, name, age, classNum);
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", stuNumbers='" + stuNumbers + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", classNum='" + classNum + '\'' +
                '}';
    }
}
