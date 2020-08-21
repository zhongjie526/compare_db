package com.prudential.DBCompare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
public class DbComparison {

    private final Logger logger = LoggerFactory.getLogger(DbComparison.class);
    private final DataSource sourceDataSource;
    private final DataSource targetDataSource;

    @Autowired
    public DbComparison(@Qualifier("source") DataSource sourceDataSource, @Qualifier("target") DataSource targetDataSource) {
        this.sourceDataSource = sourceDataSource;
        this.targetDataSource = targetDataSource;
    }

    public void rowCountChecks(String sourceTable, String targetTable) {
        JdbcTemplate sourceJdbcTemplate = new JdbcTemplate(sourceDataSource);
        JdbcTemplate targetJdbcTemplate = new JdbcTemplate(targetDataSource);

        int sourceRowCount = sourceJdbcTemplate.queryForObject("select count(*) from "+sourceTable, Integer.class);
        int targetRowCount = targetJdbcTemplate.queryForObject("select count(*) from "+targetTable, Integer.class);

        String msg = sourceRowCount == targetRowCount
                ? "Count Matched: source table: "+sourceTable+", target table: "+targetTable+", count = "+sourceRowCount
                : "Count Mismatched: source table: "+sourceTable + ", source count = "+sourceRowCount
                + ", target table: "+targetTable + ", target count = "+targetRowCount;

        logger.info(msg);

    }
}
