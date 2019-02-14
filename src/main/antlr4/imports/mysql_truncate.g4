grammar mysql_truncate;
import mysql_literal_tokens, mysql_idents;

// https://dev.mysql.com/doc/refman/8.0/en/truncate-table.html
// TRUNCATE [TABLE] tbl_name
truncate_table: TRUNCATE table_name;
