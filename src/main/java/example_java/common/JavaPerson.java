package example_java.common;

import java.io.Serializable;

public class JavaPerson implements Serializable {

    String first;
    String last;
    int age;
    String state;

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
