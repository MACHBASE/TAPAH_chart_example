# .bashrc

machsql -s localhost -u sys -p manager -P $MACHBASE_PORT_NO -f create_tag.sql
machsql -s localhost -u sys -p manager -P $MACHBASE_PORT_NO -f insert_meta.sql
