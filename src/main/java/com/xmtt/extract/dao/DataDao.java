package com.xmtt.extract.dao;

import com.xmtt.extract.util.JdbcUtil;
import com.xmtt.extract.util.PreparedStatementConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by TT on 2019/9/3.
 */
public class DataDao {

    public List<Map<String, Object>> select(Connection con, final String title) throws SQLException {

        return JdbcUtil.queryMaps(con, "select b.ID id,b.标题 title,z.内容 content from 标题 b left join 资料库 z on z.fid=b.ID where b.标题 like ? limit 5", new PreparedStatementConfig() {
            public void config(PreparedStatement ps) throws SQLException {
                ps.setString(1, title);
            }
        });
    }

}
