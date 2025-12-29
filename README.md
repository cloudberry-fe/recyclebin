# RecycleBin Extension for Cloudberry

## Overview

RecycleBin is a Cloudberry extension that provides recycle bin functionality for dropped and truncated tables. Instead of permanently deleting tables, it moves them to a designated schema, allowing data recovery and audit trail.

## Features

- **Safe DROP Operations**: Tables are moved to the recycle bin instead of being permanently deleted
- **TRUNCATE Backup**: When truncating tables, original data is backed up to the recycle bin
- **Metadata Tracking**: Complete audit trail of all operations including timestamps, user information, and table statistics
- **Configurable Retention**: Configurable retention period with automated cleanup of backup tables via scheduled tasks
- **Permission Preservation**: Maintains table permissions and ownership when moving to recycle bin during DROP operations
- **Easy Recovery**: Simple functions to restore tables from the recycle bin

## Installation

### Prerequisites

- Cloudberry 2.0+ (Tested on Cloudberry 2.0+)
- GNU Make, GCC compiler
- PostgreSQL development headers

### Compilation and Installation

```bash
# Clone the repository
git clone https://github.com/cloudberry-fe/recyclebin.git
cd recyclebin

# Compile the extension
make

# Install the extension
sudo make install

# Enable the extension in Cloudberry
# Set in `postgresql.conf`: shared_preload_libraries = 'recyclebin'
# Or
gpconfig -c shared_preload_libraries -v 'recyclebin' --coordinatoronly
# Restart service required

psql -d your_database -c "CREATE EXTENSION recyclebin;"
```

## Configuration

After installation, configure the extension via PostgreSQL GUC parameters:

```sql
-- Enable/disable extension (default: off)
SET recyclebin.enabled = on;

-- Enable/disable TRUNCATE backup (default: off)
SET recyclebin.enable_truncate_backup = off;

-- Set recycle bin schema name (default: 'recycle')
SET recyclebin.schema = 'recycle';

-- Enable/disable dropping indexes and constraints when moving to recycle bin (default: 'on')
SET recyclebin.drop_indexes_after_move = 'on';

-- Default retention days for recycle bin backup tables (default: 7 days)
SET recyclebin.retention_days = 7;
```

## Usage Examples

### Basic Usage

```sql
-- Create extension
CREATE EXTENSION recyclebin;

-- Enable recycle bin functionality
SET recyclebin.enabled = on;
SET recyclebin.enable_truncate_backup = on;

-- Create test table
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);
INSERT INTO users (name) VALUES ('Alice'), ('Bob');

-- Drop table (moved to recycle bin)
DROP TABLE users;

-- Check recycle bin
SELECT * FROM recycle.recyclebin_overview;

-- Restore table
SELECT recycle.restore_table('users_20251224144209');

-- Truncate table (backup to recycle bin)
CREATE TABLE logs (id SERIAL, message TEXT);
INSERT INTO logs (message) VALUES ('test');
TRUNCATE TABLE logs;

-- Restore table
SELECT recycle.restore_table('logs_20251224144311');

-- Query restored table
SELECT * FROM public.logs20251224145412_restored;
```

### Recycle Bin Management

```sql
-- View all tables in recycle bin
SELECT * FROM recycle.recyclebin_overview;

-- View recycle bin status
SELECT * FROM recycle.recyclebin_status;

-- Query tables older than 30 days
SELECT * FROM recycle.cleanup_recyclebin(false,30);

-- Query tables older than recyclebin.retention_days
SELECT * FROM recycle.cleanup_recyclebin(false);

-- Actually cleanup tables older than 30 days (default: recyclebin.retention_days)
SELECT * FROM recycle.cleanup_recyclebin(true,30);

-- Manually delete specific table from recycle bin
SELECT recycle.delete_table('users_20241206172210');

-- View tables pending cleanup (exceeding retention period)
SELECT * FROM recycle.recyclebin_pending_cleanup;
```

## Metadata Table Structure

The extension creates a metadata table `recycle.recyclebin_metadata` with the following structure:

| Field | Type | Description |
|-------|------|-------------|
| id | BIGSERIAL | Auto-increment primary key |
| operator_name | TEXT | User who performed the operation |
| original_schema | TEXT | Original schema of the table |
| original_table | TEXT | Original name of the table |
| recyclebin_schema | TEXT | Recycle bin schema (default: 'recycle') |
| recyclebin_table | TEXT | Table name in recycle bin |
| operation_type | TEXT | Operation type: 'DROP' or 'TRUNCATE' |
| operation_time | TIMESTAMPTZ | Operation timestamp |
| update_time | TIMESTAMPTZ | Update timestamp |
| moved_size | BIGINT | Table size in bytes |
| moved_rows | BIGINT | Estimated row count of the table |
| state | TEXT | Current state: 'RECYCLED' or 'DELETED' or 'RESTORE' |

## Function Reference

### Core Functions

- `recycle.restore_table(recyclebin_table, new_schema, new_name)` - Restore table from recycle bin (admin only)
- `recycle.cleanup_recyclebin(days_old, dry_run)` - Cleanup old tables from recycle bin (admin only)
- `recycle.delete_table(recyclebin_table)` - Manually delete table from recycle bin (admin only)

### Views

- `recycle.recyclebin_overview` - Complete overview of recycle bin contents
- `recycle.recyclebin_status` - Recycle bin statistics summary
- `recycle.recyclebin_pending_cleanup` - Tables pending cleanup

## Performance Considerations

- Extension hooks into PostgreSQL's ProcessUtility mechanism
- Table move operations are atomic and transactional
- Minimal overhead from metadata insertion
- Recycle bin queries use indexes for efficiency
- Automated cleanup designed to be non-blocking

## Troubleshooting

### Common Issues

**"permission denied for schema recycle"**

```sql
-- Grant necessary permissions
GRANT CREATE ON SCHEMA recycle TO your_user;
```

**Dropped table not appearing in recycle bin**

```sql
-- Check if enabled
SHOW recyclebin.enabled;
```

**Truncated table not appearing in recycle bin**

```sql
-- Check if enabled
SHOW recyclebin.enable_truncate_backup;
```

### Debug Information

```sql
-- Enable debug logging for extension
SET log_min_messages = DEBUG1;

-- Check extension configuration
SELECT name, setting FROM pg_settings WHERE name LIKE 'recyclebin%';
```

## License
This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a Pull Request

## Acknowledgments

- Inspired by Oracle Database's FLASHBACK and recycle bin functionality
- Referenced the pgtrashcan extension
- Built on PostgreSQL extension framework

## Support

For support, please:

1. Review the documentation
2. Search existing issues
3. Create a new issue with detailed information
