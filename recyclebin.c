#include "postgres.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include <time.h>
#include "catalog/pg_authid.h"
#include "utils/relcache.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_index.h"
#define MAX_IDENTIFIER_LEN 63

PG_MODULE_MAGIC;

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void recyclebin_ProcessUtility(PlannedStmt *pstmt,
                                      const char *queryString,
                                      bool readOnlyTree,
                                      ProcessUtilityContext context,
                                      ParamListInfo params,
                                      QueryEnvironment *queryEnv,
                                      DestReceiver *dest,
                                      QueryCompletion *qc);

static bool insert_recyclebin_metadata(const char *original_schema,
                                     const char *original_table,
                                     const char *recyclebin_table,
                                     const char *operation_type,
                                     Oid operator_oid);
static bool check_table_dependencies(const char *schema, const char *table);
static bool drop_indexes_and_constraints(const char *schema, const char *table);

static char *recyclebin_nspname = "recycle";
static bool recyclebin_enabled = true;
static bool recyclebin_enable_truncate_backup = true;
static int  recyclebin_retention_days = 7;
static bool in_truncate_hook = false;
static bool drop_indexes_after_move = true;

/* Function declarations */
static RangeVar *makeRangeVarFromAnyName(List *names);
static char *generate_recycle_name(const char *relname);
static bool table_exists_in_schema(const char *schema, const char *table);
static bool move_table_to_recyclebin(RangeVar *r);
static bool is_in_recyclebin_schema(const char *schema);
static bool backup_table_for_truncate(RangeVar *r);

void _PG_init(void);
void _PG_fini(void);

void
_PG_init(void)
{
    /* Register GUC parameters */
    DefineCustomBoolVariable("recyclebin.enabled",
                             "Enable or disable recyclebin",
                             NULL,
                             &recyclebin_enabled,
                             false,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("recyclebin.enable_truncate_backup",
                             "Enable backup of truncated tables",
                             NULL,
                             &recyclebin_enable_truncate_backup,
                             false,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomBoolVariable("recyclebin.drop_indexes_after_move",
                             "Drop indexes and constraints after moving table to recyclebin",
                             NULL,
                             &drop_indexes_after_move,
                             true,
                             PGC_SUSET,
                             0,
                             NULL,
                             NULL,
                             NULL);

    DefineCustomStringVariable("recyclebin.schema",
                               "Schema name for recyclebin",
                               NULL,
                               &recyclebin_nspname,
                               "recycle",
                               PGC_SUSET,
                               0,
                               NULL,
                               NULL,
                               NULL);
   DefineCustomIntVariable("recyclebin.retention_days",
                               "Number of days to keep tables in recyclebin",
                               NULL,
                               &recyclebin_retention_days,
                               7, 
                               0, 
                               3650,
                               PGC_SUSET,
                               0,
                               NULL,
                               NULL,
                               NULL);
    /* Install hooks */
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = recyclebin_ProcessUtility;

    elog(LOG, "recyclebin extension loaded with schema: %s", recyclebin_nspname);
}

void
_PG_fini(void)
{
    /* Restore hooks */
    if (prev_ProcessUtility)
        ProcessUtility_hook = prev_ProcessUtility;

    elog(LOG, "recyclebin extension unloaded");
}

static bool
is_in_recyclebin_schema(const char *schema)
{
    if (!schema)
        return false;

    return (strcmp(schema, recyclebin_nspname) == 0);
}

/* Check if table has dependent views */
static bool
check_table_dependencies(const char *schema, const char *table)
{
    StringInfoData sql;
    int ret;
    bool has_dependencies = false;
    
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(WARNING, "Failed to connect to SPI for checking dependencies");
        return false;
    }
    
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT n.nspname as schema_name, c.relname as table_name, v.relname as view_name "
                     "FROM pg_depend d "
                     "JOIN pg_class c ON d.refobjid = c.oid "
                     "JOIN pg_namespace n ON c.relnamespace = n.oid "
                     "JOIN pg_rewrite rw ON d.objid = rw.oid "
                     "JOIN pg_class v ON rw.ev_class = v.oid "
                     "WHERE c.relkind = 'r' "
                     "  AND v.relkind = 'v' "
                     "  AND n.nspname = '%s' "
                     "  AND c.relname = '%s' ",
                     schema, table);
    
    ret = SPI_execute(sql.data, true, 0);
    pfree(sql.data);
    
    if (ret == SPI_OK_SELECT)
    {
        if (SPI_processed > 0)
        {
            has_dependencies = true;
            
            /* Construct error messages */
            StringInfoData error_msg;
            StringInfoData detail_msg;
            StringInfoData hint_msg;
            
            initStringInfo(&error_msg);
            initStringInfo(&detail_msg);
            initStringInfo(&hint_msg);
            
            appendStringInfo(&error_msg, 
                           "cannot drop table %s.%s because other objects depend on it",
                           schema, table);
            
            /* Collect all dependent views */
            bool first = true;
            for (uint64 i = 0; i < SPI_processed; i++)
            {
                HeapTuple tuple = SPI_tuptable->vals[i];
                TupleDesc tupdesc = SPI_tuptable->tupdesc;
                
                char *view_schema = SPI_getvalue(tuple, tupdesc, 1);
                char *view_name = SPI_getvalue(tuple, tupdesc, 3);
                
                if (view_schema && view_name)
                {
                    if (first)
                    {
                        appendStringInfo(&detail_msg, "view %s.%s",
                                       view_schema, view_name);
                        first = false;
                    }
                    else
                    {
                        appendStringInfo(&detail_msg, ", %s.%s",
                                       view_schema, view_name);
                    }
                }
                
                if (view_schema)
                    pfree(view_schema);
                if (view_name)
                    pfree(view_name);
            }
            
            if (!first)
            {
                appendStringInfo(&detail_msg, " depends on table %s.%s",
                               schema, table);
            }
            
            appendStringInfo(&hint_msg, "recyclebin: DROP CASCADE is not supported. You need to drop the dependent views first, then drop the table.");
            
            /* Save error messages */
            char *final_error_msg = pstrdup(error_msg.data);
            char *final_detail_msg = pstrdup(detail_msg.data);
            char *final_hint_msg = pstrdup(hint_msg.data);
            
            /* Free memory */
            pfree(error_msg.data);
            pfree(detail_msg.data);
            pfree(hint_msg.data);
            
            /* Disconnect SPI */
            SPI_finish();
            
            /* Throw error */
            ereport(ERROR,
                   (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("%s", final_error_msg),
                    errdetail("%s", final_detail_msg),
                    errhint("%s", final_hint_msg)));
            
            pfree(final_error_msg);
            pfree(final_detail_msg);
            pfree(final_hint_msg);
        }
    }
    else
    {
        elog(WARNING, "Failed to check dependencies for table %s.%s, SPI error: %d",
             schema, table, ret);
    }
    
    SPI_finish();
    return has_dependencies;
}

/* Drop indexes and constraints on table */
static bool
drop_indexes_and_constraints(const char *schema, const char *table)
{
    StringInfoData sql;
    int ret;
    bool success = true;
    int dropped_count = 0;
    int constraint_dropped = 0;
    int index_dropped = 0;
    
    if (!drop_indexes_after_move)
    {
        elog(DEBUG1, "Index/constraint dropping is disabled by GUC setting");
        return true;
    }
    
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(WARNING, "Failed to connect to SPI for dropping indexes and constraints");
        return false;
    }
    
    elog(DEBUG1, "Starting to drop indexes and constraints for table %s.%s", schema, table);
    
    /* 1. Drop primary keys, unique constraints, foreign keys, check constraints */
    initStringInfo(&sql);
    appendStringInfo(&sql,
                 "SELECT conname "
                 "FROM pg_constraint c "
                 "JOIN pg_class t ON c.conrelid = t.oid "
                 "JOIN pg_namespace n ON t.relnamespace = n.oid "
                 "WHERE n.nspname = '%s' "
                 "  AND t.relname = '%s' "
                 "  AND c.contype IN ('p', 'u', 'f', 'c') ",
                 schema, table);

    ret = SPI_execute(sql.data, true, 0);
    if (ret == SPI_OK_SELECT)
    {
        uint64 num_foreign = SPI_processed;
        elog(DEBUG1, "Found %ld constraints to drop", (long)num_foreign);
        
        /* Collect all constraint names into array, then loop to drop */
        char **connames = NULL;
        if (num_foreign > 0)
        {
            connames = (char **)palloc(num_foreign * sizeof(char *));
            for (uint64 i = 0; i < num_foreign; i++)
            {
                HeapTuple tuple = SPI_tuptable->vals[i];
                TupleDesc tupdesc = SPI_tuptable->tupdesc;
                char *conname = SPI_getvalue(tuple, tupdesc, 1);
                if (conname)
                {
                    connames[i] = pstrdup(conname); 
                    pfree(conname);
                }
                else
                {
                    connames[i] = NULL;
                }
            }
        }
        
        /* Free SPI context */
        SPI_freetuptable(SPI_tuptable);
        
        /* Loop to drop collected constraints */
        for (uint64 i = 0; i < num_foreign; i++)
        {
            if (connames[i])
            {
                initStringInfo(&sql);
                appendStringInfo(&sql,
                                "ALTER TABLE %s.%s DROP CONSTRAINT IF EXISTS %s",
                                quote_identifier(schema),
                                quote_identifier(table),
                                quote_identifier(connames[i]));
                
                elog(DEBUG1, "Executing SQL to drop constraint: %s", sql.data);
                
                ret = SPI_execute(sql.data, false, 0);
                pfree(sql.data);
                
                if (ret != SPI_OK_UTILITY)
                {
                    elog(WARNING, "Failed to drop constraint %s on table %s.%s: %d (%s)",
                         connames[i], schema, table, ret, SPI_result_code_string(ret));
                    success = false;
                }
                else
                {
                    elog(DEBUG1, "Dropped constraint %s on table %s.%s",
                         connames[i], schema, table);
                    dropped_count++;
                    constraint_dropped++;
                }

                pfree(connames[i]);
            }

        }
        
        if (connames)
            pfree(connames);
    }
    else
    {
        elog(WARNING, "Failed to get constraints for table %s.%s, SPI error: %d (%s)",
             schema, table, ret, SPI_result_code_string(ret));
    }
        
    /* 2. Drop indexes (excluding those related to constraints) */
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT n.nspname as index_schema, c.relname as index_name "
                     "FROM pg_class c "
                     "JOIN pg_namespace n ON c.relnamespace = n.oid "
                     "JOIN pg_index i ON c.oid = i.indexrelid "
                     "JOIN pg_class t ON i.indrelid = t.oid "
                     "JOIN pg_namespace tn ON t.relnamespace = tn.oid "
                     "WHERE tn.nspname = '%s' "
                     "  AND t.relname = '%s' "
                     "  AND c.relkind = 'i' "
                     "  AND NOT EXISTS ("
                     "    SELECT 1 FROM pg_constraint co "
                     "    WHERE co.conindid = c.oid"
                     "  )",
                     schema, table);
    
    ret = SPI_execute(sql.data, true, 0);
    
    if (ret == SPI_OK_SELECT)
    {
        uint64 num_indexes = SPI_processed;
        SPITupleTable *index_tuptable = SPI_tuptable;
        
        elog(DEBUG1, "Found %ld non-constraint indexes to drop for table %s.%s", 
             (long)num_indexes, schema, table);
        
        /* Allocate arrays to save index information */
        char **index_schemas = palloc(num_indexes * sizeof(char *));
        char **index_names = palloc(num_indexes * sizeof(char *));
        
        for (uint64 i = 0; i < num_indexes; i++)
        {
            HeapTuple tuple = index_tuptable->vals[i];
            TupleDesc tupdesc = index_tuptable->tupdesc;
            index_schemas[i] = SPI_getvalue(tuple, tupdesc, 1);
            index_names[i] = SPI_getvalue(tuple, tupdesc, 2);
        }
        
        pfree(sql.data);
        
        /* Execute drop operations */
        for (uint64 i = 0; i < num_indexes; i++)
        {
            if (index_schemas[i] && index_names[i])
            {
                initStringInfo(&sql);
                appendStringInfo(&sql,
                                 "DROP INDEX %s.%s",
                                 quote_identifier(index_schemas[i]),
                                 quote_identifier(index_names[i]));
                
                elog(DEBUG1, "Executing SQL to drop index: %s", sql.data);
                
                ret = SPI_execute(sql.data, false, 0);
                pfree(sql.data);
                
                if (ret != SPI_OK_UTILITY)
                {
                    elog(WARNING, "Failed to drop index %s.%s: %d, SPI result: %s",
                         index_schemas[i], index_names[i], ret, SPI_result_code_string(ret));
                    success = false;
                }
                else
                {
                    dropped_count++;
                    index_dropped++;
                }
            }
            
            /* Free memory */
            if (index_schemas[i])
                pfree(index_schemas[i]);
            if (index_names[i])
                pfree(index_names[i]);
        }
        
        pfree(index_schemas);
        pfree(index_names);
    }
    else
    {
        pfree(sql.data);
        elog(WARNING, "Failed to get indexes for table %s.%s, SPI error: %d (%s)",
             schema, table, ret, SPI_result_code_string(ret));
    }
    
    SPI_finish();
    
    if (dropped_count > 0)
    {
        ereport(LOG,
                (errmsg("Dropped %d objects from table %s.%s (constraints: %d, indexes: %d)",
                        dropped_count, schema, table, 
                        constraint_dropped, index_dropped)));
    }
    else
    {
        ereport(DEBUG1,
                (errmsg("No indexes or constraints to drop from table %s.%s",
                        schema, table)));
    }
    
    return success;
}

static RangeVar *
makeRangeVarFromAnyName(List *names)
{
    RangeVar *r = makeNode(RangeVar);

    switch (list_length(names))
    {
        case 1:
            r->catalogname = NULL;
            r->schemaname = NULL;
            r->relname = strVal(linitial(names));
            break;
        case 2:
            r->catalogname = NULL;
            r->schemaname = strVal(linitial(names));
            r->relname = strVal(lsecond(names));
            break;
        case 3:
            r->catalogname = strVal(linitial(names));
            r->schemaname = strVal(lsecond(names));
            r->relname = strVal(lthird(names));
            break;
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("improper qualified name (too many dotted names): %s",
                            NameListToString(names))));
            break;
    }

    r->relpersistence = RELPERSISTENCE_PERMANENT;
    r->location = -1;

    return r;
}

/* Insert metadata record */
static bool
insert_recyclebin_metadata(const char *original_schema,
                         const char *original_table,
                         const char *recyclebin_table,
                         const char *operation_type,
                         Oid operator_oid)
{
    StringInfoData sql;
    int ret;
    bool success = false;
    const char *operator_name;

    operator_name = GetUserNameFromId(operator_oid, false);

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(WARNING, "Failed to connect to SPI for inserting metadata");
        return false;
    }

    /* Get size and row count of moved table */
    long moved_size = 0;
    long moved_rows = 0;

    /* Get table size */
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT pg_total_relation_size('%s.%s')",
                     quote_identifier(recyclebin_nspname),
                     quote_identifier(recyclebin_table));

    ret = SPI_execute(sql.data, true, 1);
    pfree(sql.data);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool isnull = true;
        Datum size_datum = SPI_getbinval(SPI_tuptable->vals[0], 
                                         SPI_tuptable->tupdesc, 
                                         1, &isnull);
        if (!isnull)
            moved_size = DatumGetInt64(size_datum);
        elog(DEBUG1, "Got table size for %s.%s: %ld bytes", 
             recyclebin_nspname, recyclebin_table, moved_size);
    }

    /* Get table row count */
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT MAX(COALESCE(reltuples::bigint, 0)) "
                     "FROM pg_class c left join pg_namespace n on n.oid = c.relnamespace "
                     "WHERE n.nspname in (%s,%s) and c.relname in (%s,%s)",
                     quote_literal_cstr(recyclebin_nspname),
                     quote_literal_cstr(original_schema),
                     quote_literal_cstr(recyclebin_table),
                     quote_literal_cstr(original_table));

    ret = SPI_execute(sql.data, true, 1);
    pfree(sql.data);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool isnull = true;
        Datum rows_datum = SPI_getbinval(SPI_tuptable->vals[0], 
                                         SPI_tuptable->tupdesc, 
                                         1, &isnull);
        if (!isnull)
            moved_rows = (long)DatumGetInt64(rows_datum);
    }

    /* Insert metadata record */
    initStringInfo(&sql);
    if (moved_size > 0)
    {
        appendStringInfo(&sql,
                         "INSERT INTO %s.recyclebin_metadata "
                         "(operator_name, original_schema, original_table, "
                         "recyclebin_table, operation_type, moved_size, moved_rows) "
                         "VALUES (%s, %s, %s, %s, %s, %ld, %ld)",
                         quote_identifier(recyclebin_nspname),
                         quote_literal_cstr(operator_name),
                         quote_literal_cstr(original_schema),
                         quote_literal_cstr(original_table),
                         quote_literal_cstr(recyclebin_table),
                         quote_literal_cstr(operation_type),
                         moved_size,
                         moved_rows);
    }
    else
    {
        appendStringInfo(&sql,
                         "INSERT INTO %s.recyclebin_metadata "
                         "(operator_name, original_schema, original_table, "
                         "recyclebin_table, operation_type) "
                         "VALUES (%s, %s, %s, %s, %s)",
                         quote_identifier(recyclebin_nspname),
                         quote_literal_cstr(operator_name),
                         quote_literal_cstr(original_schema),
                         quote_literal_cstr(original_table),
                         quote_literal_cstr(recyclebin_table),
                         quote_literal_cstr(operation_type));
    }

    elog(DEBUG1, "Insert metadata SQL: %s", sql.data);

    ret = SPI_execute(sql.data, false, 0);
    pfree(sql.data);

    if (ret != SPI_OK_INSERT)
    {
        elog(WARNING, "Failed to insert metadata for table %s.%s: SPI error %d (%s)",
             original_schema, original_table, ret, SPI_result_code_string(ret));
    }
    else
    {
        success = true;
        elog(DEBUG1, "Inserted metadata record for %s.%s -> %s.%s",
             original_schema, original_table, recyclebin_nspname, recyclebin_table);
    }

    SPI_finish();
    return success;
}

static bool
table_exists_in_schema(const char *schema, const char *table)
{
    Oid namespaceId;
    Oid relid;

    namespaceId = get_namespace_oid(schema, true);
    if (!OidIsValid(namespaceId))
        return false;

    relid = get_relname_relid(table, namespaceId);
    return OidIsValid(relid);
}

static char *
generate_recycle_name(const char *relname)
{
    char *name;
    char timestamp[32];
    time_t current_time;
    struct tm *tm;
    int max_base_len;
    
    if (!relname || strlen(relname) == 0)
        return NULL;
    
    max_base_len = MAX_IDENTIFIER_LEN - 20;
    
    if (max_base_len < 1) max_base_len = 1;
    
    current_time = time(NULL);
    tm = localtime(&current_time);
    
    strftime(timestamp, sizeof(timestamp), "%Y%m%d%H%M%S", tm);
    
    if (strlen(relname) > max_base_len)
    {
        char *truncated_relname;
        truncated_relname = palloc(max_base_len + 1);
        strncpy(truncated_relname, relname, max_base_len);
        truncated_relname[max_base_len] = '\0';
        
        name = psprintf("%s_%s", truncated_relname, timestamp);
        pfree(truncated_relname);
    }
    else
    {
        name = psprintf("%s_%s", relname, timestamp);
    }
    
    if (strlen(name) > MAX_IDENTIFIER_LEN)
    {
        char *truncated_name;
        truncated_name = palloc(MAX_IDENTIFIER_LEN + 1);
        strncpy(truncated_name, name, MAX_IDENTIFIER_LEN);
        truncated_name[MAX_IDENTIFIER_LEN] = '\0';
        pfree(name);
        name = truncated_name;
        elog(DEBUG1, "Truncated recycle table name to %d characters: %s", 
             MAX_IDENTIFIER_LEN, name);
    }
    
    return name;
}

static bool
move_table_to_recyclebin(RangeVar *r)
{
    StringInfoData sql;
    char *recycle_name;
    char *schema_name;
    char *table_name;
    int ret;
    bool success = false;
    int attempt = 0;
    const int max_attempts = 10;

    /* Check if table has dependent views */
    schema_name = r->schemaname ? r->schemaname : "public";
    table_name = r->relname;
    
    if (check_table_dependencies(schema_name, table_name))
    {
        return false;
    }

    /* Before moving table, drop indexes and constraints (if this feature is enabled) */
    if (drop_indexes_after_move)
    {
        elog(DEBUG1, "Attempting to drop indexes and constraints for original table %s.%s before moving to recyclebin",
             schema_name, table_name);
        
        if (!drop_indexes_and_constraints(schema_name, table_name))
        {
            elog(WARNING, "Failed to drop indexes and constraints for table %s.%s, but will continue with move operation",
                 schema_name, table_name);
        }
    }

    /* Generate recycle table name with timestamp */
    recycle_name = generate_recycle_name(r->relname);

    /* Ensure table name is unique */
    while (table_exists_in_schema(recyclebin_nspname, recycle_name) && attempt < max_attempts)
    {
        pfree(recycle_name);
        attempt++;

        char timestamp[32];
        time_t current_time;
        struct tm *tm;

        current_time = time(NULL);
        tm = localtime(&current_time);
        strftime(timestamp, sizeof(timestamp), "%Y%m%d%H%M%S", tm);

        recycle_name = psprintf("%s_%s_%d", r->relname, timestamp, attempt);
    }

    if (attempt >= max_attempts && table_exists_in_schema(recyclebin_nspname, recycle_name))
    {
        pfree(recycle_name);
        /* Use microsecond timestamp to ensure uniqueness */
        TimestampTz now = GetCurrentTimestamp();
        struct pg_tm *tm;
        int ms;

        tm = pg_localtime(&now, session_timezone);
        ms = (now % USECS_PER_SEC) / 1000;

        /* Format: original_table_name_YYYYMMDDHHMMSS_MS */
        char timestamp[32];
        snprintf(timestamp, sizeof(timestamp), "%04d%02d%02d%02d%02d%02d_%03d",
                 tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                 tm->tm_hour, tm->tm_min, tm->tm_sec, ms);

        recycle_name = psprintf("%s_%s", r->relname, timestamp);
    }

    /* Build SQL for rename and change schema */
    initStringInfo(&sql);

    /* Rename first, then change schema */
    appendStringInfo(&sql,
                     "ALTER TABLE %s.%s RENAME TO \"%s\"; "
                     "ALTER TABLE %s.\"%s\" SET SCHEMA %s",
                     quote_identifier(schema_name),
                     quote_identifier(table_name),
                     recycle_name,
                     quote_identifier(schema_name),
                     recycle_name,
                     quote_identifier(recyclebin_nspname));

    /* Use SPI to execute SQL */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(WARNING, "Failed to connect to SPI");
        pfree(recycle_name);
        pfree(sql.data);
        return false;
    }

    elog(DEBUG1, "Attempting to execute SQL: %s", sql.data);

    /* Execute multiple SQL statements */
    ret = SPI_execute(sql.data, false, 0);

    SPI_finish();
    pfree(sql.data);

    if (ret == SPI_OK_UTILITY)
    {
        ereport(NOTICE,
                (errmsg("recyclebin: table %s.%s moved to recycle for DROP: %s",
                        schema_name, table_name, recycle_name)));
        
        success = true;
        
        /* Insert metadata record */
        if (!insert_recyclebin_metadata(schema_name, table_name, recycle_name,
                                      "DROP", GetUserId()))
        {
            elog(WARNING, "Failed to insert metadata for dropped table %s.%s",
                 schema_name, table_name);
        }
    }
    else
    {
        elog(WARNING, "Failed to move table %s.%s to recyclebin", schema_name, table_name);
    }

    pfree(recycle_name);
    return success;
}

static bool
backup_table_for_truncate(RangeVar *r)
{
    StringInfoData sql;
    char *backup_name;
    char *schema_name;
    char *table_name;
    int ret;
    bool success = false;
    int attempt = 0;
    const int max_attempts = 10;

    if (!recyclebin_enabled || !recyclebin_enable_truncate_backup)
        return false;

    /* Check if table has dependent views */
    schema_name = r->schemaname ? r->schemaname : "public";
    table_name = r->relname;

    /* Generate backup table name with timestamp */
    backup_name = generate_recycle_name(r->relname);

    /* Ensure table name is unique */
    while (table_exists_in_schema(recyclebin_nspname, backup_name) && attempt < max_attempts)
    {
        pfree(backup_name);
        attempt++;

        char timestamp[32];
        time_t current_time;
        struct tm *tm;

        current_time = time(NULL);
        tm = localtime(&current_time);
        strftime(timestamp, sizeof(timestamp), "%Y%m%d%H%M%S", tm);

        backup_name = psprintf("%s_%s_%d", r->relname, timestamp, attempt);
    }

    if (attempt >= max_attempts && table_exists_in_schema(recyclebin_nspname, backup_name))
    {
        pfree(backup_name);
        /* Use microsecond timestamp to ensure uniqueness */
        TimestampTz now = GetCurrentTimestamp();
        struct pg_tm *tm;
        int ms;

        tm = pg_localtime(&now, session_timezone);
        ms = (now % USECS_PER_SEC) / 1000;

        char timestamp[32];
        snprintf(timestamp, sizeof(timestamp), "%04d%02d%02d%02d%02d%02d_%03d",
                 tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                 tm->tm_hour, tm->tm_min, tm->tm_sec, ms);

        backup_name = psprintf("%s_%s", r->relname, timestamp);
    }

    /* Use SPI to execute SQL */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(WARNING, "Failed to connect to SPI for TRUNCATE backup");
        pfree(backup_name);
        return false;
    }

    /* Build SQL for copying table - execute in two steps */
    initStringInfo(&sql);
    
    appendStringInfo(&sql,
                     "CREATE TABLE %s.\"%s\" (LIKE %s.%s INCLUDING COMMENTS) WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)",
                     recyclebin_nspname, backup_name,
                     quote_identifier(schema_name),
                     quote_identifier(table_name));
    
    elog(DEBUG1, "Attempting to execute SQL: %s", sql.data);

    ret = SPI_execute(sql.data, false, 0);
    
    if (ret != SPI_OK_UTILITY)
    {
        elog(WARNING, "Failed to create backup table for TRUNCATE table %s.%s, SPI error %d (%s)",
             schema_name, table_name, ret, SPI_result_code_string(ret));
        
        pfree(sql.data);
        SPI_finish();
        pfree(backup_name);
        return false;
    }
    
    pfree(sql.data);
    
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "INSERT INTO %s.\"%s\" SELECT * FROM %s.%s",
                     recyclebin_nspname, backup_name,
                     quote_identifier(schema_name),
                     quote_identifier(table_name));
    
    elog(DEBUG1, "Attempting to execute SQL: %s", sql.data);

    ret = SPI_execute(sql.data, false, 0);
    pfree(sql.data);
    
    SPI_finish();

    if (ret == SPI_OK_INSERT || ret == SPI_OK_SELECT)
    {
        ereport(NOTICE,
                (errmsg("recyclebin: table %s.%s backed up to recycle for TRUNCATE: %s",
                        schema_name, table_name, backup_name)));
        success = true;
        
        /* Insert metadata record */
        if (!insert_recyclebin_metadata(schema_name, table_name, backup_name,
                                      "TRUNCATE", GetUserId()))
        {
            elog(WARNING, "Failed to insert metadata for truncated table %s.%s",
                 schema_name, table_name);
        }
    }
    else
    {
        elog(WARNING, "Failed to insert data into backup for TRUNCATE table %s.%s, SPI error %d (%s)",
             schema_name, table_name, ret, SPI_result_code_string(ret));
    }

    pfree(backup_name);
    return success;
}

static void
recyclebin_ProcessUtility(PlannedStmt *pstmt,
                          const char *queryString,
                          bool readOnlyTree,
                          ProcessUtilityContext context,
                          ParamListInfo params,
                          QueryEnvironment *queryEnv,
                          DestReceiver *dest,
                          QueryCompletion *qc)
{
    Node *parsetree;

    if (!recyclebin_enabled)
    {
        goto pass_through;
    }

    parsetree = pstmt->utilityStmt;

    if (!parsetree)
    {
        goto pass_through;
    }

    if (in_truncate_hook)
    {
        goto pass_through;
    }

    /* Process DROP TABLE command */
    if (nodeTag(parsetree) == T_DropStmt)
    {
        DropStmt *stmt = (DropStmt *) parsetree;

        if (stmt->removeType == OBJECT_TABLE)
        {
            ListCell *cell;
            bool is_cascade = (stmt->behavior == DROP_CASCADE);
            bool missing_ok = false;
            bool all_success = true;
            bool all_in_recyclebin = true;

            missing_ok = stmt->missing_ok;

            /* First check if all tables are in recyclebin */
            foreach(cell, stmt->objects)
            {
                List *name = (List *) lfirst(cell);
                RangeVar *r = makeRangeVarFromAnyName(name);

                if (!is_in_recyclebin_schema(r->schemaname))
                {
                    all_in_recyclebin = false;
                    break;
                }
            }

            if (all_in_recyclebin)
            {
                elog(DEBUG1, "All tables are in recyclebin, allowing DROP");
                goto pass_through;
            }

            if (is_cascade)
            {
                ereport(WARNING,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("recyclebin: DROP CASCADE is not supported, table will be dropped permanently")));
                goto pass_through;
            }

            /* For each table to be dropped, move it to recyclebin */
            foreach(cell, stmt->objects)
            {
                List *name = (List *) lfirst(cell);
                RangeVar *r = makeRangeVarFromAnyName(name);

                /* If table is already in recyclebin, skip */
                if (is_in_recyclebin_schema(r->schemaname))
                    continue;

                /* Check if table exists */
                if (!missing_ok)
                {
                    Oid namespaceId = r->schemaname ?
                        get_namespace_oid(r->schemaname, true) :
                        get_namespace_oid("public", true);

                    if (namespaceId == InvalidOid ||
                        !OidIsValid(get_relname_relid(r->relname, namespaceId)))
                    {
                        ereport(ERROR,
                                (errcode(ERRCODE_UNDEFINED_TABLE),
                                 errmsg("table \"%s\" does not exist",
                                        r->schemaname ?
                                        psprintf("%s.%s", r->schemaname, r->relname) :
                                        r->relname)));
                    }
                }

                /* Move table to recyclebin */
                if (!move_table_to_recyclebin(r))
                {
                    all_success = false;
                    break;
                }
            }

            /* If all tables successfully moved, do not execute original DROP */
            if (all_success)
            {
                if (qc)
                {
                    qc->commandTag = CMDTAG_UNKNOWN;
                }
                return;
            }
            else
            {
                ereport(WARNING,
                        (errmsg("recyclebin: failed to move some tables to recyclebin, performing original DROP")));
                goto pass_through;
            }
        }
    }
    /* Process TRUNCATE command */
    else if (nodeTag(parsetree) == T_TruncateStmt)
    {
        TruncateStmt *stmt = (TruncateStmt *) parsetree;
        
        /* Check if CASCADE option is present */
        bool has_cascade = (stmt->behavior == DROP_CASCADE);
        
        if (has_cascade)
        {
            /* If CASCADE option exists, execute original TRUNCATE directly */
            elog(WARNING, "recyclebin: TRUNCATE CASCADE is not supported, table will be truncate permanently");
            goto pass_through;
        }
        else if (recyclebin_enable_truncate_backup && stmt->relations)
        {
            ListCell *lc;
            bool all_backup_success = true;
            
            foreach(lc, stmt->relations)
            {
                RangeVar *r = (RangeVar *) lfirst(lc);
                
                if (is_in_recyclebin_schema(r->schemaname))
                    continue;
                
                if (!backup_table_for_truncate(r))
                {
                    all_backup_success = false;
                    break;
                }
            }
            
            if (all_backup_success)
            {
                in_truncate_hook = true;
                
                if (prev_ProcessUtility)
                    (*prev_ProcessUtility)(pstmt, queryString, readOnlyTree, context,
                                          params, queryEnv, dest, qc);
                else
                    standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
                                           params, queryEnv, dest, qc);
                
                in_truncate_hook = false;
                return;
            }
            else
            {
                ereport(WARNING,
                        (errmsg("recyclebin: failed to backup tables for TRUNCATE, performing original TRUNCATE")));
                goto pass_through;
            }
        }
    }

pass_through:
    /* Call original processing function */
    if (prev_ProcessUtility)
        (*prev_ProcessUtility)(pstmt, queryString, readOnlyTree, context,
                              params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
                               params, queryEnv, dest, qc);
}