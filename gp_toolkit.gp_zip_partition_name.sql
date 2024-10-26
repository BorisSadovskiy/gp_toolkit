DROP FUNCTION IF EXISTS gp_toolkit.gp_zip_partition_name(p_schema_name varchar, p_table_name varchar, p_part_name varchar, p_tablespace varchar);
DROP FUNCTION IF EXISTS gp_toolkit.gp_zip_table_partition(p_schema_name varchar, p_table_name varchar, p_part_name varchar);
DROP FUNCTION IF EXISTS gp_toolkit.gp_zip_partition(p_schema_name varchar, p_table_name varchar, p_part_name varchar, p_tablespace varchar);

CREATE OR REPLACE FUNCTION gp_toolkit.gp_zip_partition(p_schema_name varchar, p_table_name varchar, p_part_name varchar, p_tablespace varchar default 'pg_default'::varchar)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
	-- SET enable_seqscan = 'off'
	-- SET enable_bitmapscan = 'off'
AS $function$
declare
    schema_name varchar(63):= substr(trim(both ' ' from p_schema_name),1,63);
    table_name varchar(46):= substr(trim(both ' ' from p_table_name),1,46);
    part_name varchar(10):= substr(trim(both ' ' from p_part_name),1,10);
    part_tablespace varchar(63):= lower(substr(trim(both ' ' from p_tablespace),1,63));
    tablespaces varchar[]:= array['pg_default', 'coldstorage'];
    table_part_name varchar(63);
    schema_table_part_name varchar(63);
    tmp_table_name varchar(127);
    v_sql text;
BEGIN
    -- Check normalized input parameters
    IF (coalesce(schema_name,'') = '') or (coalesce(table_name,'') = '') or (coalesce(part_name,'') = '') THEN
        raise notice '% ERROR some parameters are empty: schema_name="%", table_name="%", part_name="%"', timeofday()::timestamp, schema_name, table_name, part_name;
        RETURN;
    END IF;
    -- Check and normalize tablespace value
    IF not (part_tablespace = ANY(tablespaces)) THEN
        part_tablespace:= tablespaces[1]; -- set pg_default
    END IF;
    raise notice '% INFO start gp_toolkit.gp_zip_partition_name() with params: %, %, %, %', timeofday()::timestamp, schema_name, table_name, part_name, p_tablespace;
    -- Get table partition name
    select pp.partitiontablename into table_part_name from pg_partitions pp 
    where pp.schemaname = schema_name and pp.tablename = table_name and pp.partitionname = part_name;
    IF NOT FOUND THEN
        raise notice '% ERROR table partition NOT FOUND: schema_name="%", table_name="%", part_name="%"', timeofday()::timestamp, schema_name, table_name, part_name;
        RETURN;
    END IF;
    -- Get full table partition name with schema
    schema_table_part_name:= '"' || schema_name || '"."' || table_part_name || '"';
    raise notice '% INFO table partition name is: %', timeofday()::timestamp, schema_table_part_name;
    -- TMP table name
    tmp_table_name:= '"tmp"."' || schema_name || '_' || table_part_name || '"';
    raise notice '% INFO tmp table name is: %', timeofday()::timestamp, tmp_table_name;
    -- DROP IF EXISTS tmp table
    v_sql:= 'drop table if exists ' || tmp_table_name || ';';
    raise notice '% INFO %', timeofday()::timestamp, v_sql;
	execute v_sql;
    -- CREATE tmp table
    v_sql:= 'create table ' || tmp_table_name || ' (like "' || schema_name || '"."' || table_name || '"' ||
        ' including defaults including constraints) WITH (appendonly=true, orientation=column, compresstype=zlib, compresslevel=4)' ||
        ' TABLESPACE ' || part_tablespace || ';';
    raise notice '% INFO %', timeofday()::timestamp, v_sql;
	execute v_sql;
    -- INSERT INTO tmp table
    v_sql:= 'insert into ' || tmp_table_name || ' select * from ' || schema_table_part_name || ';';
    raise notice '% INFO %', timeofday()::timestamp, v_sql;
	execute v_sql;
    -- !!! EXCHANGE PARTITION with tmp table !!!
    v_sql:= 'alter table "' || schema_name || '"."' || table_name || '"' ||
        ' EXCHANGE PARTITION "' || part_name || '" WITH TABLE ' || tmp_table_name || ' WITH VALIDATION;';
    raise notice '% INFO %', timeofday()::timestamp, v_sql;
	execute v_sql;
    -- FINALLY DROP tmp table
    v_sql:= 'drop table ' || tmp_table_name || ';';
    raise notice '% INFO %', timeofday()::timestamp, v_sql;
	execute v_sql;
    -- Success
    raise notice '% INFO end gp_toolkit.gp_zip_partition_name() success', timeofday()::timestamp;
    RETURN;
EXCEPTION
    when others then
        RAISE NOTICE '% ERROR: SQLSTATE: %, SQLERRM: %', timeofday()::timestamp, SQLSTATE, SQLERRM;
        RETURN;
END;                         
$function$;
