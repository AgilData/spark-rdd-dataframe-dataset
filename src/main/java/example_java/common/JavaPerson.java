package example_java.common;

import java.io.Serializable;

public class JavaPerson implements Serializable {

    String first;
    String last;
    int age;
    String state;

    public JavaPerson() {
    }

    public JavaPerson(String first, String last, int age, String state) {
        this.first = first;
        this.last = last;
        this.age = age;
        this.state = state;
    }

    public String getFirst() {
        return first;
    }

    public String getLast() {
        return last;
    }

    public int getAge() {
        return age;
    }

    public String getState() {
        return state;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public void setLast(String last) {
        this.last = last;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "JavaPerson{" +
                "first='" + first + '\'' +
                ", last='" + last + '\'' +
                ", age=" + age +
                ", state='" + state + '\'' +
                '}';
    }
}
