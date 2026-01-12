import type { 
  PostgresCredentials, 
  PostgresConnectionResponse, 
  PostgresSchema,
  PostgresTable,
  SchemaCompatibility 
} from './types';
import type { SurveyCTOField } from './types';
import { mockSchemas, delay, shouldFail } from './mockData';

// Connection state
let currentConnection: { credentials: PostgresCredentials } | null = null;

/**
 * Tests PostgreSQL connection and returns available schemas/tables
 * 
 * Real implementation would call:
 * POST /api/pg/connect
 * Body: { host, port, database, username, password, sslMode }
 */
export async function connectPostgres(
  credentials: PostgresCredentials
): Promise<PostgresConnectionResponse> {
  // Simulate network latency (1000ms - 2500ms for DB connection)
  await delay(1000 + Math.random() * 1500);

  // Validate input
  if (!credentials.host?.trim()) {
    return {
      success: false,
      error: 'Host is required',
    };
  }

  if (!credentials.database?.trim()) {
    return {
      success: false,
      error: 'Database name is required',
    };
  }

  if (!credentials.username?.trim()) {
    return {
      success: false,
      error: 'Username is required',
    };
  }

  if (!credentials.password?.trim()) {
    return {
      success: false,
      error: 'Password is required',
    };
  }

  // Simulate connection failures for specific test cases
  if (credentials.host === 'invalid.host') {
    return {
      success: false,
      error: 'Could not resolve hostname. Please check the host address.',
    };
  }

  if (credentials.database === 'nonexistent') {
    return {
      success: false,
      error: `Database "${credentials.database}" does not exist`,
    };
  }

  if (credentials.password === 'wrongpassword') {
    return {
      success: false,
      error: 'Password authentication failed',
    };
  }

  // Simulate connection timeout
  if (shouldFail(0.05)) {
    return {
      success: false,
      error: 'Connection timed out. Please check if the database server is accessible.',
    };
  }

  // Store connection
  currentConnection = { credentials };

  return {
    success: true,
    schemas: mockSchemas,
  };
}

/**
 * Fetches all schemas from the connected database
 * 
 * Real implementation would call:
 * GET /api/pg/schemas
 */
export async function fetchSchemas(): Promise<PostgresSchema[]> {
  await delay(300 + Math.random() * 300);

  if (!currentConnection) {
    throw new Error('Not connected to database');
  }

  return mockSchemas;
}

/**
 * Fetches tables for a specific schema
 * 
 * Real implementation would call:
 * GET /api/pg/schemas/:schemaName/tables
 */
export async function fetchTables(schemaName: string): Promise<PostgresTable[]> {
  await delay(200 + Math.random() * 200);

  if (!currentConnection) {
    throw new Error('Not connected to database');
  }

  const schema = mockSchemas.find(s => s.name === schemaName);
  return schema?.tables || [];
}

/**
 * Validates schema compatibility between SurveyCTO form and target table
 * 
 * Real implementation would call:
 * POST /api/pg/validate-schema
 * Body: { formFields, targetSchema, targetTable }
 */
export async function validateSchemaCompatibility(
  formFields: SurveyCTOField[],
  targetSchema: string,
  targetTable: string
): Promise<SchemaCompatibility> {
  await delay(400 + Math.random() * 400);

  if (!currentConnection) {
    throw new Error('Not connected to database');
  }

  const schema = mockSchemas.find(s => s.name === targetSchema);
  const table = schema?.tables.find(t => t.name === targetTable);

  if (!table) {
    // New table - always compatible
    return {
      compatible: true,
      missingColumns: [],
      extraColumns: [],
      typeMismatches: [],
      primaryKeyMatch: true,
    };
  }

  // Check for missing columns (in form but not in table)
  const tableColumnNames = table.columns.map(c => c.name.toLowerCase());
  const formFieldNames = formFields.map(f => f.name.toLowerCase());
  
  const missingColumns = formFields
    .filter(f => !tableColumnNames.includes(f.name.toLowerCase()))
    .map(f => f.name);

  // Check for extra columns (in table but not in form) - excluding common metadata
  const metadataColumns = ['created_at', 'updated_at', 'id'];
  const extraColumns = table.columns
    .filter(c => 
      !formFieldNames.includes(c.name.toLowerCase()) && 
      !metadataColumns.includes(c.name.toLowerCase())
    )
    .map(c => c.name);

  // Check primary key match
  const formPK = formFields.find(f => f.isPrimaryKey);
  const primaryKeyMatch = formPK ? 
    table.primaryKey?.toLowerCase() === formPK.name.toLowerCase() || 
    table.primaryKey?.toLowerCase() === 'id' : 
    false;

  return {
    compatible: missingColumns.length === 0 && primaryKeyMatch,
    missingColumns,
    extraColumns,
    typeMismatches: [], // Simplified for mock
    primaryKeyMatch,
  };
}

/**
 * Creates a new table based on SurveyCTO form fields
 * 
 * Real implementation would call:
 * POST /api/pg/tables
 * Body: { schemaName, tableName, columns }
 */
export async function createTable(
  schemaName: string,
  tableName: string,
  formFields: SurveyCTOField[]
): Promise<{ success: boolean; error?: string }> {
  await delay(800 + Math.random() * 500);

  if (!currentConnection) {
    return { success: false, error: 'Not connected to database' };
  }

  // Check if table already exists
  const schema = mockSchemas.find(s => s.name === schemaName);
  if (schema?.tables.find(t => t.name === tableName)) {
    return { success: false, error: `Table "${schemaName}.${tableName}" already exists` };
  }

  // In real implementation, would execute CREATE TABLE statement
  // For mock, we just return success
  return { success: true };
}

/**
 * Disconnects from the database
 */
export function disconnect(): void {
  currentConnection = null;
}

/**
 * Checks if there's an active database connection
 */
export function isConnected(): boolean {
  return currentConnection !== null;
}

/**
 * Maps SurveyCTO field types to PostgreSQL types
 */
export function mapFieldTypeToPostgres(fieldType: SurveyCTOField['type']): string {
  const typeMap: Record<SurveyCTOField['type'], string> = {
    text: 'TEXT',
    integer: 'INTEGER',
    decimal: 'NUMERIC',
    date: 'DATE',
    datetime: 'TIMESTAMPTZ',
    select_one: 'TEXT',
    select_multiple: 'TEXT[]',
    geopoint: 'GEOGRAPHY(POINT, 4326)',
    calculate: 'TEXT',
  };
  return typeMap[fieldType] || 'TEXT';
}
