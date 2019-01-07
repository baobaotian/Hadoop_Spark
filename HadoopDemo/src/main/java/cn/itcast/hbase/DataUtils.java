package cn.itcast.hbase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

public class DataUtils {
    public static <T> ArrayList<T> load(String path,Class<T> c){
        ArrayList<T> lists=new ArrayList<>();
        FileReader fileReader =null;
        BufferedReader bufferedReader=null;
        try {
            fileReader=new FileReader(path);
            bufferedReader=new BufferedReader(fileReader);
            String line;
            while((line = bufferedReader.readLine()) != null){
                String[] split = line.split(",");
                //根据类对象创建对象
                T t = c.newInstance();
                //获取属性列表
                Field[] fields = c.getDeclaredFields();
                for(int i=0;i < fields.length;i++){
                    String value=split[i];
                    //获取属性名
                    String name =fields[i].getName();
                    //获取属性列表
                    Class type =fields[i].getType();
                    //拼接属性值
                    //拼接设置属性值的方法  setName()
                    String methodName ="set"+name.substring(0,1).toUpperCase()+name.substring(1);
                    //设置方法名和参数类型获取方法的对象
                    Method method = c.getMethod(methodName, type);
                    //执行方法，给属性设置值
                    if(type.getName().equals("java.lang.Integer")){
                        method.invoke(t,Integer.parseInt(value));
                    }else{
                        method.invoke(t,value);
                    }

                }
                lists.add(t);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }finally {
            if(fileReader != null){
                try {
                    fileReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(bufferedReader != null){
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return lists;

    }
}
