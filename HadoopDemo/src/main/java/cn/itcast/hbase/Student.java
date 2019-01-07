package cn.itcast.hbase;

public class Student {
    private String id;
    private String name ;
    private Integer age;
    private String gender;
    private String calzz;

    public Student(String id, String name, Integer age, String gender, String calzz) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.gender = gender;
        this.calzz = calzz;
    }

    public String getId() {
        return id;
    }

    public Student setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Student setName(String name) {
        this.name = name;
        return this;
    }

    public Integer getAge() {
        return age;
    }

    public Student setAge(Integer age) {
        this.age = age;
        return this;
    }

    public String getGender() {
        return gender;
    }

    public Student setGender(String gender) {
        this.gender = gender;
        return this;
    }

    public String getCalzz() {
        return calzz;
    }

    public Student setCalzz(String calzz) {
        this.calzz = calzz;
        return this;
    }
    //无参构造一定要写的，如果不写会出很多问题：使用student对象没有有参的都会出错
    public Student() {
    }

    @Override
    public String toString() {
        return "Student{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age='" + age + '\'' +
                ", gender='" + gender + '\'' +
                ", calzz='" + calzz + '\'' +
                '}';
    }
}
