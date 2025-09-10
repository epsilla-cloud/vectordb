# Epsilla VectorDB API Documentation

This document lists all available API endpoints for the Epsilla Vector Database.

## Core Database Management APIs

### Database Operations
- **POST** `/api/load` - Load/Create a database
  - Body: `{ "name": "db_name", "path": "/db/path", "vectorScale": 150000, "walEnabled": true }`
  - Headers: OpenAI, JinaAI, VoyageAI, MixedBreadAI, Nomic, MistralAI API keys

- **POST** `/api/{db_name}/unload` - Unload a database
- **POST** `/api/{db_name}/release` - Release database memory
- **DELETE** `/api/{db_name}/drop` - Drop a database
- **POST** `/api/dump` - Dump database to file
  - Body: `{ "name": "db_name", "path": "/dump/path" }`

### Table Schema Operations
- **POST** `/api/{db_name}/schema/tables` - Create a table
  - Body: Table schema with fields, indices, auto-embeddings
  - Optional: `returnTableId: true`

- **DELETE** `/api/{db_name}/schema/tables/{table_name}` - Drop a table
- **GET** `/api/{db_name}/schema/tables/{table_name}/describe` - Describe table schema (TODO)
- **GET** `/api/{db_name}/schema/tables/show` - List all tables in database
- **GET** `/api/{db_name}/schema/tables` - RESTful endpoint to list tables

## Data Operations

### Insert/Update Operations
- **POST** `/api/{db_name}/data/insert` - Insert records
  - Body: `{ "table": "table_name", "data": [...], "upsert": false }`
  - Headers: Embedding service API keys

- **POST** `/api/{db_name}/data/insertprepare` - Prepare insert operation
  - Body: `{ "table": "table_name", "primaryKeys": [...] }`

### Query Operations
- **POST** `/api/{db_name}/data/query` - Vector similarity search
  - Body: `{ "table": "table_name", "queryVector": [...], "limit": 10, "filter": "", "withDistance": false }`
  - Alternative: `{ "table": "table_name", "query": "text content", ... }`
  - Support for: facets, response fields, queryField/queryIndex

- **POST** `/api/{db_name}/data/get` - Project/Get records
  - Body: `{ "table": "table_name", "response": [...], "filter": "", "limit": 100, "skip": 0, "primaryKeys": [...] }`

### Delete Operations
- **POST** `/api/{db_name}/data/delete` - Delete records
  - Body: `{ "table": "table_name", "primaryKeys": [...] }` OR `{ "table": "table_name", "filter": "condition" }`

### Utility Operations
- **POST** `/api/{db_name}/data/load` - Load CSV data (TODO)

## Statistics & Monitoring

- **GET** `/api/{db_name}/statistics` - Get database statistics
- **GET** `/api/{db_name}/records/count?table={table_name}` - Get record count for database/table
- **GET** `/api/records/count?table={table_name}` - Get record count across all databases

## System Operations

### Administration
- **POST** `/api/rebuild` - Rebuild indices on-demand
- **POST** `/api/{db_name}/compact` - Compact database/table
  - Body: `{ "tableName": "optional", "threshold": 0.3 }`
- **POST** `/api/compact` - Compact all databases
  - Body: `{ "threshold": 0.3 }`

### Configuration
- **POST** `/api/setleader` - Set leadership status
  - Body: `{ "leader": true }`
- **POST** `/api/config` - Update configuration
  - Body: Configuration JSON object

## Configuration Management APIs (ConfigController)

- **GET** `/api/config` - Get all configuration values
- **GET** `/api/config/{key}` - Get specific configuration value
- **PUT** `/api/config/{key}` - Set configuration value
- **POST** `/api/config/batch` - Update multiple configuration values
- **DELETE** `/api/config/{key}` - Delete configuration value
- **GET** `/api/config/schema` - Get configuration schema
- **POST** `/api/config/export` - Export configuration to file
- **POST** `/api/config/import` - Import configuration from file

## Security & Health

- **GET** `/` - Welcome message
- **GET** `/state` - Server health status
- **GET** `/health` - Health check (SecureWebController)
- **GET** `/admin/auth/stats` - API key statistics (admin only)

## Swagger Documentation

- **GET** `/api-docs/oas-3.0.0.json` - OpenAPI 3.0 specification
- **GET** `/api/docs` - Swagger UI root
- **GET** `/api/docs/{filename}` - Swagger UI resources

## Supported Embedding Providers

The API supports the following embedding service headers:
- `X-OpenAI-Api-Key`: OpenAI API key
- `X-JinaAI-Api-Key`: JinaAI API key  
- `X-VoyageAI-Api-Key`: VoyageAI API key
- `X-MixedBreadAI-Api-Key`: MixedBreadAI API key
- `X-Nomic-Api-Key`: Nomic API key
- `X-MistralAI-Api-Key`: MistralAI API key

## Vector Types Supported

- **Dense Vectors**: Arrays of floats `[0.1, 0.2, 0.3, ...]`
- **Sparse Vectors**: Objects with indices and values `{ "indices": [1, 5, 10], "values": [0.1, 0.2, 0.3] }`

## Data Types Supported

- VECTOR_FLOAT, VECTOR_DOUBLE (with dimensions)
- STRING, INTEGER, BOOLEAN
- Primary key support
- Metric types: COSINE, EUCLIDEAN, DOT_PRODUCT

## Default Configuration

- Default server port: 8888
- Default vector scale: 150000 (configurable via EPSILLA_INITIAL_CAPACITY environment variable)
- Default WAL: enabled
- Default embedding service: http://localhost:8889
- Default delete mode: soft delete (configurable via SOFT_DELETE environment variable)

## Delete Modes

### Soft Delete (Default)
- Records marked as deleted using bitset, data remains in memory
- Faster deletion operations
- Memory reclaimed through compaction process
- Use `SOFT_DELETE=true` (default)

### Hard Delete (Concurrent-Safe)
- Records physically removed from memory immediately
- Thread-safe batch deletion with exclusive locking
- Immediate memory reclamation with data compaction
- Primary key index rebuilt after deletion for consistency
- Slower deletion due to data shifting operations
- Use `SOFT_DELETE=false`

#### Concurrent Safety Features
- **Exclusive Write Lock**: Prevents concurrent access during deletion
- **Batch Processing**: Multiple records deleted in single atomic operation  
- **Index Rebuilding**: Primary key mappings reconstructed after compaction
- **Data Integrity**: No race conditions or partial data states

### Configuration Options
- **Environment Variable**: `export SOFT_DELETE=false`
- **Runtime API**: `POST /api/config` with `{"SoftDelete": false}`
- **Configuration API**: `PUT /api/config/SoftDelete` with `{"value": "false"}`