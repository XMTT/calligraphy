package com.xmtt.extract;

import com.xmtt.extract.dao.DataDao;
import com.xmtt.extract.handler.Handler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by TT on 2019/9/3.
 */
public class Main {

    public static Connection getCon() throws SQLException {
        Connection con;
        con = DriverManager.getConnection("jdbc:sqlite:E:/XMTT/calligraphy/经典珍藏中国书法传世珍品3000幅/db");
        return con;

    }

    public static void main(String[] args) throws SQLException, IOException {
        DataDao dataDao = new DataDao();
        Handler handler = new Handler();
        handler.setDataDao(dataDao);
        handler.setCon(getCon());
        handler.setFile("E:/XMTT/calligraphy");
        handler.select("李白");
    }


}
