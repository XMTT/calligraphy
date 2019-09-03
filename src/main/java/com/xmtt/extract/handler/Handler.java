package com.xmtt.extract.handler;

import com.xmtt.extract.dao.DataDao;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.zip.InflaterInputStream;

/**
 * Created by TT on 2019/9/3.
 */
public class Handler {
    private DataDao dataDao;
    private Connection con;
    private String file = "E:/XMTT/calligraphy";

    public DataDao getDataDao() {
        return dataDao;
    }

    public void setDataDao(DataDao dataDao) {
        this.dataDao = dataDao;
    }

    public Connection getCon() {
        return con;
    }

    public void setCon(Connection con) {
        this.con = con;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public void select(String key)throws SQLException {
        List<Map<String, Object>> list = dataDao.select(con, "%"+key+"%");
        for (Map<String, Object> map : list) {
            String title = (String) map.get("title");
            //System.out.println(map.get("content"));
            File file = new File(this.file, title.trim() + ".jpg");
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(file);
                System.out.println(map.get("content").getClass());
                InputStream is = (InputStream) map.get("content");
                handler(is, fos);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public void handler(InputStream is,OutputStream fos) {
            try {
                try {
                    extract(is, fos, new byte[100]);
                    fos.flush();
                }finally {
                    fos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
    }


    public static void extract(InputStream is, OutputStream os, byte[] buf) throws IOException {
        //流数据zlib解压
        InflaterInputStream iis = new InflaterInputStream(is);
        int len,count = buf.length;
        int un = 1474, idx = 0, ret;
        byte[] head = new byte[un];
        //截掉文件头
        while (un > 0) {
            ret = iis.read(head, idx, un);
            if (ret >= 0) {
                idx += ret;
                un -= ret;
            } else {
                throw new IOException("a");
            }
        }
        //boolean head = true;
        while ((len = iis.read(buf)) >= 0) {
            //每次都要读取偶数位的长度
            while (len < count&&len!=0) {
                len += iis.read(buf, len, count - len);
            }
            //os.write(buf, 0, len);
            //解压之后的流转成十六进制的字符串
            String decompressHexString = bytesToHexString(buf);
            //十六进制的字符串翻译（解释）成字符串
            String hexString = hexStringToString(decompressHexString);
            if (hexString != null) {
                //截掉文件尾巴
                if (hexString.indexOf("-1") > 0) {
                    String content = hexString.substring(0, hexString.indexOf("-1")).trim();
                    //字符串再转字节流，写入文件
                    byte[] b = toBytes(content);
                    os.write(b, 0, b.length);
                    break;
                } else {
                    byte[] b = toBytes(hexString.trim());
                    os.write(b, 0, b.length);
                    buf = new byte[count];
                }
            }
        }
    }

    public static String hexStringToString(String s) {
        if (s == null || s.equals("")) {
            return null;
        }
        s = s.replace(" ", "");
        byte[] baKeyword = new byte[s.length() / 2];
        for (int i = 0; i < baKeyword.length; i++) {
            try {
                baKeyword[i] = (byte) (0xff & Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            s = new String(baKeyword, "UTF-8");
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return s;
    }

    //字节流转十六进制的字符串
    public static String bytesToHexString(byte[] src) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (src == null || src.length <= 0) {
            return null;
        }
        for (int i = 0; i < src.length; i++) {
            int v = src[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2) {
                stringBuilder.append(0);
            }
            stringBuilder.append(hv);
        }
        return stringBuilder.toString();
    }

    //十六进制字符串转成字节流
    public static byte[] toBytes(String str) {
        if (str == null || str.trim().equals("")) {
            return new byte[0];
        }

        byte[] bytes = new byte[str.length() / 2];
        for (int i = 0; i < str.length() / 2; i++) {
            String subStr = str.substring(i * 2, i * 2 + 2);
            try {
                bytes[i] = (byte) Integer.parseInt(subStr, 16);
            } catch (Exception e) {
                System.out.println(str);
            }
        }

        return bytes;
    }



    public static byte[] hex2byte(String str) { // 字符串转二进制
        if (str == null) {
            return null;
        }
        return str.getBytes();
    }

    public static String byte2hex(byte[] b) // 二进制转字符串
    {
        StringBuffer sb = new StringBuffer();
        String stmp = "";
        for (int n = 0; n < b.length; n++) {
            stmp = Integer.toHexString(b[n] & 0XFF);
            if (stmp.length() == 1) {
                sb.append("0" + stmp);
            } else {
                sb.append(stmp);
            }

        }
        return sb.toString();
    }

    public static byte[] readStream(InputStream in, boolean close) throws IOException {
        byte[] var3;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            copy(in, out, new byte[8192]);
            var3 = out.toByteArray();
        } finally {
            if (close) {
                in.close();
            }

        }

        return var3;
    }

    public static void copy(InputStream in, OutputStream os, byte[] buf) throws IOException {
        boolean var3 = false;

        int len;
        while ((len = in.read(buf)) >= 0) {
            if (len > 0) {
                os.write(buf, 0, len);
            }
        }

    }


    public static String xor(String content) {
        content = change(content);
        String[] b = content.split(" ");
        int a = 0;
        for (int i = 0; i < b.length; i++) {
            a = a ^ Integer.parseInt(b[i], 16);
        }
        if (a < 10) {
            StringBuffer sb = new StringBuffer();
            sb.append("0");
            sb.append(a);
            return sb.toString();
        }
        return Integer.toHexString(a);
    }

    public static String change(String content) {
        String str = "";
        for (int i = 0; i < content.length(); i++) {
            if (i % 2 == 0) {
                str += " " + content.substring(i, i + 1);
            } else {
                str += content.substring(i, i + 1);
            }
        }
        System.out.println(str.trim());
        return str.trim();
    }




}
