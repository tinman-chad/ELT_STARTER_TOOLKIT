# --Upsert sql formats
#MySQL
mysql_sql = f"""INSERT INTO {table} ({fields})
    VALUES (
        {values}
    ) ON DUPLICATE KEY
    UPDATE
        {field_value_list};"""
#Postgres     
postgres_sql = f"""INSERT INTO {table} ({fields})
    VALUES (
        {values}
    ) ON CONFLICT ({primary_key}) DO
    UPDATE SET
        {field_value_list};"""
#MS SQL
mssql_sql = f"""MERGE {table} AS target_{table}
    USING (
        VALUES ({values}) AS source ({fields}) 
            ON {temp_table}.{primary_key_field} = source.{primary_key_field}
        UPDATE SET
            {field_value_list}
        WHEN NOT MATCHED THEN
        INSERT ({fields})
        VALUES ({prepended_source_fields})        
    );"""
    
#always safest to do that transform/load step from staging table into target table, rather than source table into target table directly.
update_stmt_3 = ", ".join(f' "{c}" ' for c in columns )
insert_stmt_1 = f' INSERT INTO {table_name} ( {update_stmt_3} ) '
insert_stmt_2 = f' Select * from {temp_table} '
insert_stmt_3 = f' ON CONFLICT ("{primary_key}") '
insert_stmt_4 = f' DO UPDATE SET '
update_stmt_1 = ", ".join(update)