package data.storm.t1;

import java.io.*;
import java.util.Random;

public class Data {
    private static String[] createTime = {"2018-10-01 12:21:59",
            "2018-10-02 12:21:59","2018-10-03 12:21:59","2018-10-04 12:21:59",
            "2018-10-05 12:21:59","2018-10-06 12:21:59","2018-10-07 12:21:59"};
    private static String[] productNames = {"外星人显示器","机箱","鼠标","键盘","海盗船内存条","煊赫门","手表","戒指","项链"};
    private static String[] prices = {"100","2000","12000","500","18"};
    public static void createLog(){
        File targetFile = new File("trace.log");
        try{
            Random random = new Random();
            StringBuffer sb = new StringBuffer();
            for(int i=0;i<100;i++){
                sb.append(createTime[random.nextInt(createTime.length)]).append("\t");
                sb.append(productNames[random.nextInt(productNames.length)]).append("\t");
                sb.append(prices[random.nextInt(prices.length)]).append("\r\n");
            }
            FileWriter fw = new FileWriter(targetFile);
            fw.write(sb.toString());
            fw.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        createLog();
    }
}
