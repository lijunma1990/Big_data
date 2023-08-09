package dx.ict.pojo;


/**
 * 实体类 用于存储数据库中的数据 javabean
 * create by xiax.xpu on
 *
 * @Date 2019/3/21 20:27
 */
public class USER_Example {
    private int id;
    private String name;
    private String phone_number;

    public USER_Example(int id, String name, String phone_number) {
        this.id = id;
        this.name = name;
        this.phone_number = phone_number;
    }

    public int getid() {
        return id;
    }

    public void setid(int id) {
        this.id = id;
    }

    public String getname() {
        return name;
    }

    public void setname(String name) {
        this.name = name;
    }

    public String getphonenumber() {
        return phone_number;
    }

    public void setphonenumber(String phonenumber) {
        this.phone_number = phonenumber;
    }


    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", stuname='" + name + '\'' +
                ", stuaddr='" + phone_number + '\'' +
                '\'' +
                '}';
    }
}