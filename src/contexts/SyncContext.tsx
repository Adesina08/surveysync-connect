import { createContext, useContext, useState, ReactNode } from 'react';
import type { SurveyCTOForm, PostgresSchema } from '@/api/types';

interface SyncState {
  // Auth state
  sessionToken: string | null;
  forms: SurveyCTOForm[];
  
  // Selection state
  selectedForm: SurveyCTOForm | null;
  
  // Database state
  schemas: PostgresSchema[];
  selectedSchema: string | null;
  selectedTable: string | null;
  createNewTable: boolean;
  newTableName: string;
  
  // Sync config
  syncMode: 'insert' | 'upsert';
}

interface SyncContextType {
  state: SyncState;
  setSessionToken: (token: string | null) => void;
  setForms: (forms: SurveyCTOForm[]) => void;
  setSelectedForm: (form: SurveyCTOForm | null) => void;
  setSchemas: (schemas: PostgresSchema[]) => void;
  setSelectedSchema: (schema: string | null) => void;
  setSelectedTable: (table: string | null) => void;
  setCreateNewTable: (create: boolean) => void;
  setNewTableName: (name: string) => void;
  setSyncMode: (mode: 'insert' | 'upsert') => void;
  reset: () => void;
}

const initialState: SyncState = {
  sessionToken: null,
  forms: [],
  selectedForm: null,
  schemas: [],
  selectedSchema: null,
  selectedTable: null,
  createNewTable: false,
  newTableName: '',
  syncMode: 'upsert',
};

const SyncContext = createContext<SyncContextType | null>(null);

export function SyncProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<SyncState>(initialState);

  const setSessionToken = (token: string | null) => 
    setState(prev => ({ ...prev, sessionToken: token }));
  
  const setForms = (forms: SurveyCTOForm[]) => 
    setState(prev => ({ ...prev, forms }));
  
  const setSelectedForm = (form: SurveyCTOForm | null) => 
    setState(prev => ({ ...prev, selectedForm: form }));
  
  const setSchemas = (schemas: PostgresSchema[]) => 
    setState(prev => ({ ...prev, schemas }));
  
  const setSelectedSchema = (schema: string | null) => 
    setState(prev => ({ ...prev, selectedSchema: schema, selectedTable: null }));
  
  const setSelectedTable = (table: string | null) => 
    setState(prev => ({ ...prev, selectedTable: table }));
  
  const setCreateNewTable = (create: boolean) => 
    setState(prev => ({ ...prev, createNewTable: create, selectedTable: null }));
  
  const setNewTableName = (name: string) => 
    setState(prev => ({ ...prev, newTableName: name }));
  
  const setSyncMode = (mode: 'insert' | 'upsert') => 
    setState(prev => ({ ...prev, syncMode: mode }));
  
  const reset = () => setState(initialState);

  return (
    <SyncContext.Provider value={{
      state,
      setSessionToken,
      setForms,
      setSelectedForm,
      setSchemas,
      setSelectedSchema,
      setSelectedTable,
      setCreateNewTable,
      setNewTableName,
      setSyncMode,
      reset,
    }}>
      {children}
    </SyncContext.Provider>
  );
}

export function useSyncContext() {
  const context = useContext(SyncContext);
  if (!context) {
    throw new Error('useSyncContext must be used within a SyncProvider');
  }
  return context;
}
