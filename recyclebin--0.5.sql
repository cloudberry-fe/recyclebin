/* recyclebin--0.5.sql */

-- Create recycle schema
CREATE SCHEMA IF NOT EXISTS recycle;

-- Grant USAGE permission on recycle schema to PUBLIC
GRANT USAGE ON SCHEMA recycle TO PUBLIC;

-- Grant CREATE permission on recycle schema to PUBLIC (allows moving tables to recycle)
GRANT CREATE ON SCHEMA recycle TO PUBLIC;

-- Notify user that extension is installed
COMMENT ON EXTENSION recyclebin IS 'Move dropped/truncated tables to a recycle schema with metadata tracking';

-- Create metadata table
CREATE TABLE IF NOT EXISTS recycle.recyclebin_metadata (
    id SERIAL PRIMARY KEY,
    operator_name TEXT NOT NULL,
    original_schema TEXT NOT NULL,
    original_table TEXT NOT NULL,
    recyclebin_schema TEXT NOT NULL DEFAULT 'recycle',
    recyclebin_table TEXT NOT NULL,
    operation_type TEXT NOT NULL CHECK (operation_type IN ('DROP', 'TRUNCATE')),
    operation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP,
    moved_size BIGINT,
    moved_rows BIGINT,
    state TEXT NOT NULL DEFAULT 'RECYCLED' CHECK (state IN ('RECYCLED', 'DELETED','RESTORE','CLEAN'))
);

-- Grant SELECT permission on metadata table to PUBLIC
-- GRANT SELECT ON recycle.recyclebin_metadata TO PUBLIC;

-- Grant USAGE permission on SEQUENCE to PUBLIC
GRANT USAGE ON SEQUENCE recycle.recyclebin_metadata_id_seq TO PUBLIC;

-- Grant INSERT permission on metadata table to PUBLIC
GRANT INSERT ON recycle.recyclebin_metadata TO PUBLIC;

-- Create indexes for faster queries
CREATE INDEX IF NOT EXISTS recyclebin_metadata_operation_time_idx 
    ON recycle.recyclebin_metadata (operation_time DESC);
    
CREATE INDEX IF NOT EXISTS recyclebin_metadata_original_table_idx 
    ON recycle.recyclebin_metadata (original_schema, original_table);
    
CREATE INDEX IF NOT EXISTS recyclebin_metadata_operator_idx 
    ON recycle.recyclebin_metadata (operator_name);

-- Create view for easier querying
CREATE OR REPLACE VIEW recycle.recyclebin_overview AS
SELECT 
    id,
    operator_name,
    original_schema || '.' || original_table AS original_table,
    recyclebin_table AS recyclebin_table,
    operation_type,
    operation_time,
    pg_size_pretty(moved_size) AS table_size,
    moved_rows,
    update_time,
    state
FROM recycle.recyclebin_metadata
ORDER BY operation_time DESC;

-- Grant SELECT permission on view to PUBLIC
-- GRANT SELECT ON recycle.recyclebin_overview TO PUBLIC;

-- Create function to restore tables
CREATE OR REPLACE FUNCTION recycle.restore_table(
    p_recyclebin_table TEXT,
    p_new_schema TEXT DEFAULT NULL,
    p_new_name TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_record RECORD;
    v_target_schema TEXT;
    v_target_table TEXT;
    v_original_schema TEXT;
    v_original_table TEXT;
    v_sql TEXT;
BEGIN
    -- Get metadata
    SELECT original_schema, original_table
    INTO v_original_schema, v_original_table
    FROM recycle.recyclebin_metadata
    WHERE recyclebin_table = p_recyclebin_table
    ORDER BY operation_time DESC
    LIMIT 1;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % not found in recyclebin metadata', p_recyclebin_table;
    END IF;
    
    -- Determine target schema
    IF p_new_schema IS NOT NULL THEN
        v_target_schema := p_new_schema;
    ELSE
        v_target_schema := v_original_schema;
    END IF;
    
    -- Determine target table name
    IF p_new_name IS NOT NULL THEN
        v_target_table := p_new_name;
    ELSE
        -- Check if original table name is already taken
        IF EXISTS (
            SELECT 1 FROM pg_tables 
            WHERE schemaname = v_target_schema 
            AND tablename = v_original_table
        ) THEN
            -- If original table name is taken, add timestamp
            v_target_table := substr(v_original_table || 
                to_char(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS'),1,54)||'_restored';
        ELSE
            v_target_table := v_original_table;
        END IF;
    END IF;
    
    -- Restore table from recycle
    v_sql := format(
        'ALTER TABLE recycle.%I SET SCHEMA %I; ' ||
        'ALTER TABLE %I.%I RENAME TO %I',
        p_recyclebin_table,
        v_target_schema,
        v_target_schema,
        p_recyclebin_table,
        v_target_table
    );
    
    EXECUTE v_sql;
    
    -- Update metadata record
    UPDATE recycle.recyclebin_metadata 
    SET state = 'RESTORE',
        update_time = CURRENT_TIMESTAMP
    WHERE recyclebin_table = p_recyclebin_table;
    
    RETURN format('Table restored as %s.%s', v_target_schema, v_target_table);
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Failed to restore table: %', SQLERRM;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant EXECUTE permission on restore function to PUBLIC
-- GRANT EXECUTE ON FUNCTION recycle.restore_table(TEXT, TEXT, TEXT) TO PUBLIC;

-- Create function to clean up recyclebin
CREATE OR REPLACE FUNCTION recycle.cleanup_recyclebin(
    p_days_old NUMERIC DEFAULT 30,
    p_dry_run BOOLEAN DEFAULT FALSE
) RETURNS TABLE (
    recyclebin_table TEXT,
    original_table TEXT,
    operation_time TIMESTAMP WITH TIME ZONE,
    moved_size BIGINT,
    table_size_pretty TEXT,
    deleted BOOLEAN,
    error_message TEXT
) AS $$
DECLARE
    v_record RECORD;
    v_sql TEXT;
    v_deleted BOOLEAN;
    v_error_message TEXT;
    v_total_size BIGINT := 0;
    v_total_tables INTEGER := 0;
    v_deleted_tables INTEGER := 0;
BEGIN
    -- Log start time
    RAISE NOTICE 'Starting recyclebin cleanup: older than % days, dry run: %', p_days_old, p_dry_run;
    
    -- Query tables that need cleanup
    FOR v_record IN 
        SELECT 
            tm.recyclebin_table,
            tm.original_schema || '.' || tm.original_table AS original_table,
            tm.operation_time,
            tm.moved_size,
            tm.id
        FROM recycle.recyclebin_metadata tm
        WHERE tm.state = 'RECYCLED'
        AND tm.operation_time < (CURRENT_TIMESTAMP - (p_days_old || ' days')::INTERVAL)
        ORDER BY tm.operation_time ASC  -- Start cleaning from oldest
    LOOP
        v_deleted := FALSE;
        v_error_message := NULL;
        
        BEGIN
            -- If not dry run, execute delete
            IF p_dry_run THEN
                -- Build SQL to drop table
                v_sql := format('DROP TABLE IF EXISTS recycle.%I CASCADE', v_record.recyclebin_table);
                
                -- Execute delete
                EXECUTE v_sql;
                
                -- Update metadata status
                UPDATE recycle.recyclebin_metadata 
                SET state = 'DELETED',
                update_time = CURRENT_TIMESTAMP
                WHERE id = v_record.id;
                
                v_deleted := TRUE;
                v_deleted_tables := v_deleted_tables + 1;
            END IF;
            
            -- Accumulate total size
            IF v_record.moved_size IS NOT NULL THEN
                v_total_size := v_total_size + v_record.moved_size;
            END IF;
            
            v_total_tables := v_total_tables + 1;
            
        EXCEPTION WHEN OTHERS THEN
            v_error_message := SQLERRM;
            RAISE WARNING 'Failed to delete table recycle.%: %', v_record.recyclebin_table, v_error_message;
        END;
        
        -- Return result
        recyclebin_table := v_record.recyclebin_table;
        original_table := v_record.original_table;
        operation_time := v_record.operation_time;
        moved_size := v_record.moved_size;
        table_size_pretty := pg_size_pretty(v_record.moved_size);
        deleted := v_deleted;
        error_message := v_error_message;
        
        RETURN NEXT;
    END LOOP;
    
    -- Output statistics
    IF NOT p_dry_run THEN
        RAISE NOTICE 'Dry run completed: Found % tables to delete, total size: %', 
                     v_total_tables, pg_size_pretty(v_total_size);
    ELSE
        RAISE NOTICE 'Cleanup completed: Deleted % tables out of % found, freed space: %', 
                     v_deleted_tables, v_total_tables, pg_size_pretty(v_total_size);
    END IF;
    
    -- If no tables found
    IF v_total_tables = 0 THEN
        RAISE NOTICE 'No tables found older than % days', p_days_old;
    END IF;
    
    RETURN;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant EXECUTE permission
-- GRANT EXECUTE ON FUNCTION recycle.cleanup_recyclebin(INTEGER, BOOLEAN) TO PUBLIC;

-- Create view for current recyclebin status
CREATE OR REPLACE VIEW recycle.recyclebin_status AS
SELECT 
    state,
    COUNT(*) as table_count,
    pg_size_pretty(SUM(COALESCE(moved_size, 0))) as total_size,
    MIN(operation_time) as oldest,
    MAX(operation_time) as newest,
    SUM(COALESCE(moved_rows, 0)) as total_rows
FROM recycle.recyclebin_metadata
GROUP BY state
ORDER BY state;

-- Grant SELECT permission
GRANT SELECT ON recycle.recyclebin_status TO PUBLIC;

-- Create view for pending cleanup tables
CREATE OR REPLACE VIEW recycle.recyclebin_pending_cleanup AS
SELECT 
    tm.recyclebin_table,
    tm.original_schema || '.' || tm.original_table AS original_table,
    tm.operation_type,
    tm.operation_time,
    EXTRACT(DAY FROM (CURRENT_TIMESTAMP - tm.operation_time))::INTEGER as days_old,
    tm.moved_size,
    pg_size_pretty(tm.moved_size) as table_size_pretty,
    tm.moved_rows,
    tm.operator_name,
    tm.state
FROM recycle.recyclebin_metadata tm
WHERE tm.state = 'RECYCLED'
ORDER BY tm.operation_time ASC;

-- Grant SELECT permission
-- GRANT SELECT ON recycle.recyclebin_pending_cleanup TO PUBLIC;

-- Create function to manually delete a single table
CREATE OR REPLACE FUNCTION recycle.delete_table(
    p_recyclebin_table TEXT
) RETURNS TEXT AS $$
DECLARE
    v_original_table TEXT;
    v_sql TEXT;
BEGIN
    -- Verify table exists and is in RECYCLED state
    IF NOT EXISTS (
        SELECT 1 
        FROM recycle.recyclebin_metadata 
        WHERE recyclebin_table = p_recyclebin_table 
        AND state = 'RECYCLED'
    ) THEN
        RAISE EXCEPTION 'Table % not found in recyclebin or already deleted', p_recyclebin_table;
    END IF;
    
    -- Get original table name for message
    SELECT original_schema || '.' || original_table INTO v_original_table
    FROM recycle.recyclebin_metadata 
    WHERE recyclebin_table = p_recyclebin_table;
    
    -- Delete table
    v_sql := format('DROP TABLE IF EXISTS recycle.%I CASCADE', p_recyclebin_table);
    EXECUTE v_sql;
    
    -- Update status
    UPDATE recycle.recyclebin_metadata 
    SET state = 'DELETED' 
    WHERE recyclebin_table = p_recyclebin_table;
    
    RETURN format('Deleted table recycle.%s (originally %s)', p_recyclebin_table, v_original_table);
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Failed to delete table: %', SQLERRM;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Grant EXECUTE permission
-- GRANT EXECUTE ON FUNCTION recycle.delete_table(TEXT) TO PUBLIC;
