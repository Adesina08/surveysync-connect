import type { 
  PostgresCredentials, 
  PostgresConnectionResponse, 
  PostgresSchema,
  PostgresTable,
  SchemaCompatibility 
} from './types';
import type { SurveyCTOField } from './types';
import { apiRequest } from './http';

// Connection state
let isDbConnected = false;

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

  try {
    const response = await apiRequest<PostgresConnectionResponse>('/api/pg/connect', {
      method: 'POST',
      body: JSON.stringify(credentials),
    });

    isDbConnected = Boolean(response.success);
    return response;
  } catch (error) {
    isDbConnected = false;
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Connection failed',
    };
  }
}

/**
 * Fetches all schemas from the connected database
 * 
 * Real implementation would call:
 * GET /api/pg/schemas
 */
export async function fetchSchemas(): Promise<PostgresSchema[]> {
  if (!isDbConnected) {
    throw new Error('Not connected to database');
  }

  return apiRequest<PostgresSchema[]>('/api/pg/schemas');
}

/**
 * Fetches tables for a specific schema
 * 
 * Real implementation would call:
 * GET /api/pg/schemas/:schemaName/tables
 */
export async function fetchTables(schemaName: string): Promise<PostgresTable[]> {
  if (!isDbConnected) {
    throw new Error('Not connected to database');
  }

  return apiRequest<PostgresTable[]>(
    `/api/pg/schemas/${encodeURIComponent(schemaName)}/tables`
  );
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
  if (!isDbConnected) {
    throw new Error('Not connected to database');
  }

  return apiRequest<SchemaCompatibility>('/api/pg/validate-schema', {
    method: 'POST',
    body: JSON.stringify({
      formFields,
      targetSchema,
      targetTable,
    }),
  });
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
  if (!isDbConnected) {
    return { success: false, error: 'Not connected to database' };
  }

  try {
    const response = await apiRequest<{ success: boolean; error?: string }>('/api/pg/tables', {
      method: 'POST',
      body: JSON.stringify({
        schemaName,
        tableName,
        columns: formFields.map(field => ({
          name: field.name,
          type: mapFieldTypeToPostgres(field.type),
          nullable: !field.isPrimaryKey,
          isPrimaryKey: field.isPrimaryKey,
        })),
      }),
    });
    return response;
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Failed to create table',
    };
  }
}

/**
 * Disconnects from the database
 */
export function disconnect(): void {
  isDbConnected = false;
}

/**
 * Checks if there's an active database connection
 */
export function isConnected(): boolean {
  return isDbConnected;
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
