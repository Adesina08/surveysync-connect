// API Types matching FastAPI backend contract

export interface SurveyCTOCredentials {
  serverName: string;
  username: string;
  password: string;
}

export interface SurveyCTOForm {
  id: string;
  name: string;
  version: string;

  // Optional metadata (backend may not provide yet)
  responses?: number;
  lastUpdated?: string;
  fields?: SurveyCTOField[];
}


export interface SurveyCTOField {
  name: string;
  type: 'text' | 'integer' | 'decimal' | 'date' | 'datetime' | 'select_one' | 'select_multiple' | 'geopoint' | 'calculate';
  label: string;
  isPrimaryKey: boolean;
}

export interface SurveyCTOAuthResponse {
  success: boolean;
  sessionToken?: string;
  forms?: SurveyCTOForm[];
  error?: string;
}

export interface PostgresCredentials {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  sslMode: 'require' | 'prefer' | 'disable';
}

export interface PostgresSchema {
  name: string;
  tables: PostgresTable[];
}

export interface PostgresTable {
  name: string;
  columns: PostgresColumn[];
  primaryKey: string | null;
  rowCount: number;
}

export interface PostgresColumn {
  name: string;
  type: string;
  nullable: boolean;
  isPrimaryKey: boolean;
}

export interface PostgresConnectionResponse {
  success: boolean;
  schemas?: PostgresSchema[];
  error?: string;
}

export interface SchemaCompatibility {
  compatible: boolean;
  missingColumns: string[];
  extraColumns: string[];
  typeMismatches: { field: string; expected: string; actual: string }[];
  primaryKeyMatch: boolean;
}

export interface SyncJobConfig {
  formId: string;
  targetSchema: string;
  targetTable: string;
  syncMode: 'insert' | 'upsert' | 'replace';
  primaryKeyField?: string;
  createNewTable: boolean;
}

export interface SyncProgress {
  jobId: number;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';
  processedRecords: number;
  totalRecords: number;
  insertedRecords: number;
  updatedRecords: number;
  errors: SyncError[];
  startedAt?: string;
  completedAt?: string;
}


export interface SyncError {
  recordId: string;
  field?: string;
  message: string;
}

export interface ApiError {
  code: string;
  message: string;
  details?: Record<string, unknown>;
}
