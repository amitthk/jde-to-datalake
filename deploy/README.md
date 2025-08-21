# Consolidated Scripts Documentation

This directory contains consolidated management scripts for the JDE-to-Bakery-Operations system.

## Directory Structure

```
deploy/
├── helpers/
│   └── consolidated_helpers.py       # All helper script management
└── README.md                         # This file
```

## Helper Management  

### consolidated_helpers.py

A unified interface for all helper scripts across the system.

#### Features:
- ✅ **Helper discovery** and function listing
- ✅ **Connectivity testing** for each helper
- ✅ **Dynamic module loading**
- ✅ **Unified interface** for all helpers
- ✅ **Verbose and dry-run modes**

#### Available Helpers:

| Helper | File | Description |
|--------|------|-------------|
| `jde` | `jde_helper.py` | JDE system integration functions |
| `bakery_system` | `bakery_helper.py` | Bakery system API interactions |  
| `bakery_ops` | `bakery_ops_helper.py` | Bakery operations helper functions |
| `session` | `session_helper.py` | Session management functions |
| `s3` | `s3_helper.py` | S3 data lake operations |
| `utility` | `utility.py` | General utility functions |

#### Usage:

```bash
# Show information about all helpers
python deploy/helpers/consolidated_helpers.py

# List functions in a specific helper
python deploy/helpers/consolidated_helpers.py --helper=jde --action=list

# Test connectivity for a helper
python deploy/helpers/consolidated_helpers.py --helper=bakery_system --action=test

# Get helper information
python deploy/helpers/consolidated_helpers.py --helper=s3 --action=info
```

#### Main Functions by Helper:

**JDE Helper:**
- `fetch_existing_ingredient`
- `create_new_ingredient` 
- `fetch_existing_ingredient_batch`
- `submit_ingredient_batch_action`

**Bakery System Helper:**
- `get_data_from_bakery_system`
- `fetch_existing_ingredient_by_id`
- `get_streamlined_action_data`
- `process_api_data`

**Session Helper:**
- `create_session`
- `get_session`
- `update_session`
- `cleanup_sessions`

**S3 Helper:**
- `upload_to_s3`
- `download_from_s3`
- `list_s3_objects`
- `sync_data_to_s3`

**Utility Helper:**
- `retry_request`
- `normalize_quantity_for_transaction_id`
- `preserve_quantity_precision`
- `validate_environment`

## Benefits of Consolidation

### 🎯 **Simplified Management**
- Single entry point for operations
- Unified helper interface
- Consistent command-line interface

### 🔒 **Better Error Handling**
- Comprehensive error reporting
- Rollback capabilities
- Dry-run previews

### 📊 **Better Logging**
- Detailed operation logs
- Progress tracking
- Success/failure summaries

### 🧪 **Testing Support**
- Connectivity testing
- Dry-run modes
- Validation checks

### 🚀 **Easier Deployment**
- Single command interface
- Dependency checking
- Environment validation

## Environment Requirements

The consolidated scripts require the same environment variables as the individual scripts:

- `PG_DATABASE_URL` - PostgreSQL connection string
- `DB_NAME` - Database name (defaults to "ingredient_db")  
- `OUTLET_ID` - Outlet identifier (formerly WINERY_ID)
- `BAKERY_SYSTEM_BASE_URL` - Bakery system API URL
- `BAKERY_SYSTEM_API_TOKEN` - API authentication token

## Error Handling

Both consolidated scripts include comprehensive error handling:

- **Connection validation** before operations
- **Detailed error messages** with context
- **Exit codes** for automation (0=success, 1=failure)

## Future Enhancements

Planned improvements for the consolidated scripts:

- [ ] **Configuration file support** (JSON/YAML config)
- [ ] **Backup/restore functionality** 
- [ ] **Scheduling support** (cron integration)
- [ ] **Web interface** for management
- [ ] **Notification system** (email/Slack on completion)
- [ ] **Performance metrics** and timing

## Troubleshooting

### Common Issues:

1. **Import errors**: Ensure the backend directory is accessible
2. **Database connection**: Verify `PG_DATABASE_URL` is correct
3. **Missing environment variables**: Check `.env` file configuration
4. **Permission issues**: Ensure scripts have execute permissions

### Debug Mode:

```bash
# Test individual helpers
python deploy/helpers/consolidated_helpers.py --helper=utility --action=test --verbose
```
