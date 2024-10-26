DROP FUNCTION IF EXISTS gp_toolkit.gp_zip_table(p_schema_name varchar, p_table_name varchar, p_drop_old bool);
DROP FUNCTION IF EXISTS gp_toolkit.gp_zip_table(p_schema_name varchar, p_table_name varchar, p_drop_old bool, p_tablespace varchar);

CREATE OR REPLACE FUNCTION gp_toolkit.gp_zip_table(p_schema_name varchar, p_table_name varchar, p_drop_old bool default true, p_tablespace varchar default 'pg_default'::varchar)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
	-- SET enable_seqscan = 'off'
	-- SET enable_bitmapscan = 'off'
AS $function$
declare
    v_schema_name varchar(63):= substr(trim(both ' ' from p_schema_name),1,63);
    v_table_name varchar(59):= substr(trim(both ' ' from p_table_name),1,59);
    table_space varchar(63):= lower(substr(trim(both ' ' from p_tablespace),1,63));
    tablespaces varchar[]:= array['pg_default', 'coldstorage'];
    zip_table_suffix varchar(4):= '_zip';
    old_table_suffix varchar(4):= '_old';
    full_table_name varchar(127);
    zip_table_name varchar(127);
    v_sql text;
BEGIN
    -- Check normalized input parameters
    IF (coalesce(v_schema_name,'') = '') or (coalesce(v_table_name,'') = '') THEN
        raise notice '% ERROR some parameters are empty: schema_name="%", table_name="%"', timeofday()::timestamp, v_schema_name, v_table_name;
        RETURN;
    END IF;
    -- Check and normalize tablespace value
    IF not (table_space = ANY(tablespaces)) THEN
        table_space:= tablespaces[1]; -- set pg_default
    END IF;
    raise notice '% INFO start gp_toolkit.gp_zip_table() with params: %, %, %, %', timeofday()::timestamp, v_schema_name, v_table_name, p_drop_old, table_space;
    select '"' || t.table_schema || '"."' || t.table_name || '"' into full_table_name
    from information_schema.tables t where t.table_catalog::varchar = current_database()::varchar
        and t.table_schema = v_schema_name and t.table_name = v_table_name;
    IF NOT FOUND THEN
        raise notice '% ERROR table name NOT FOUND: schema_name="%", table_name="%"', timeofday()::timestamp, v_schema_name, v_table_name;
        RETURN;
    END IF;
    raise notice '% INFO table name is: %', timeofday()::timestamp, full_table_name;
    -- ZIP table name
    zip_table_name:= '"' || v_schema_name || '"."' || v_table_name || zip_table_suffix || '"';
    raise notice '% INFO zip table name is: %', timeofday()::timestamp, zip_table_name;
    -- DROP IF EXISTS zip table
    v_sql:= 'drop table if exists ' || zip_table_name || ';';
    raise notice '% INFO %', timeofday()::timestamp, v_sql;
	execute v_sql;
    -- CREATE zip table
    v_sql:= 'create table ' || zip_table_name || ' (like ' || full_table_name || ' including defaults including constraints)' ||
        ' WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=4) TABLESPACE ' || table_space || ';';
    raise notice '% INFO %', timeofday()::timestamp, v_sql;
	execute v_sql;
    -- INSERT INTO zip table
    v_sql:= 'insert into ' || zip_table_name || ' select * from ' || full_table_name || ';';
    raise notice '% INFO %', timeofday()::timestamp, v_sql;
	execute v_sql;
    -- RENAME orig table to old
    v_sql:= 'alter table ' || full_table_name || ' rename to "' || v_table_name || old_table_suffix || '";';
    raise notice '% INFO %', timeofday()::timestamp, v_sql;
	execute v_sql;
    -- RENAME zipped table to orig
    v_sql:= 'alter table ' || zip_table_name || ' rename to "' || v_table_name || '";';
    raise notice '% INFO %', timeofday()::timestamp, v_sql;
	execute v_sql;
    -- DROP old table 
    if p_drop_old then
        v_sql:= 'drop table "' || v_schema_name || '"."' || v_table_name || old_table_suffix || '";';
        raise notice '% INFO %', timeofday()::timestamp, v_sql;
	    execute v_sql;
    end if;
    -- Success
    raise notice '% INFO end gp_toolkit.gp_zip_table() success', timeofday()::timestamp;
    RETURN;
EXCEPTION
    when others then
        RAISE NOTICE '% ERROR: SQLSTATE: %, SQLERRM: %', timeofday()::timestamp, SQLSTATE, SQLERRM;
        RETURN;
END;                         
$function$;