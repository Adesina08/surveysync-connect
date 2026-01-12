import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Table, Plus, ArrowRight, Key, Layers, CheckCircle2, AlertTriangle } from "lucide-react";
import { cn } from "@/lib/utils";
import { useSyncContext } from "@/contexts/SyncContext";

interface TableConfigurationProps {
  onContinue: () => void;
}

const TableConfiguration = ({ onContinue }: TableConfigurationProps) => {
  const {
    state,
    setSelectedSchema,
    setSelectedTable,
    setCreateNewTable,
    setNewTableName,
    setSyncMode,
  } = useSyncContext();

  const {
    schemas,
    selectedSchema,
    selectedTable,
    createNewTable,
    newTableName,
    syncMode,
    selectedForm,
  } = state;

  // Get tables for selected schema
  const currentSchema = schemas.find(s => s.name === selectedSchema);
  const tables = currentSchema?.tables || [];

  const isValid = selectedSchema && (createNewTable ? newTableName.trim() : selectedTable);

  return (
    <Card className="w-full max-w-xl mx-auto shadow-card border-border/50 animate-fade-in">
      <CardHeader className="pb-4">
        <div className="w-14 h-14 rounded-xl gradient-primary flex items-center justify-center mx-auto mb-4 shadow-card">
          <Table className="w-7 h-7 text-primary-foreground" />
        </div>
        <CardTitle className="text-xl text-center">Configure Target Table</CardTitle>
        <CardDescription className="text-center">
          Choose where to store your synced data
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="space-y-3">
          <Label className="text-sm font-medium">Schema</Label>
          <Select value={selectedSchema || ""} onValueChange={setSelectedSchema}>
            <SelectTrigger>
              <SelectValue placeholder="Select a schema" />
            </SelectTrigger>
            <SelectContent>
              {schemas.map((s) => (
                <SelectItem key={s.name} value={s.name}>
                  {s.name}
                  <span className="text-muted-foreground ml-2">
                    ({s.tables.length} tables)
                  </span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-3">
          <Label className="text-sm font-medium">Table Selection</Label>
          <div className="grid grid-cols-2 gap-3">
            <button
              onClick={() => setCreateNewTable(false)}
              className={cn(
                "p-4 rounded-lg border text-left transition-all",
                !createNewTable
                  ? "border-primary bg-primary/5 shadow-card"
                  : "border-border hover:border-primary/50"
              )}
            >
              <Layers className={cn(
                "w-5 h-5 mb-2",
                !createNewTable ? "text-primary" : "text-muted-foreground"
              )} />
              <p className="font-medium text-sm">Use Existing Table</p>
              <p className="text-xs text-muted-foreground mt-1">
                Sync to an existing PostgreSQL table
              </p>
            </button>
            <button
              onClick={() => setCreateNewTable(true)}
              className={cn(
                "p-4 rounded-lg border text-left transition-all",
                createNewTable
                  ? "border-primary bg-primary/5 shadow-card"
                  : "border-border hover:border-primary/50"
              )}
            >
              <Plus className={cn(
                "w-5 h-5 mb-2",
                createNewTable ? "text-primary" : "text-muted-foreground"
              )} />
              <p className="font-medium text-sm">Create New Table</p>
              <p className="text-xs text-muted-foreground mt-1">
                Auto-generate table from form schema
              </p>
            </button>
          </div>
        </div>

        {!createNewTable ? (
          <div className="space-y-3">
            <Label className="text-sm font-medium">Select Table</Label>
            {tables.length === 0 && selectedSchema ? (
              <div className="flex items-center gap-2 p-3 rounded-lg bg-muted/50 border border-border">
                <AlertTriangle className="w-4 h-4 text-muted-foreground" />
                <p className="text-sm text-muted-foreground">
                  No tables in this schema. Create a new table instead.
                </p>
              </div>
            ) : (
              <Select value={selectedTable || ""} onValueChange={setSelectedTable}>
                <SelectTrigger>
                  <SelectValue placeholder="Choose a table" />
                </SelectTrigger>
                <SelectContent>
                  {tables.map((t) => (
                    <SelectItem key={t.name} value={t.name}>
                      {t.name}
                      <span className="text-muted-foreground ml-2">
                        ({t.rowCount.toLocaleString()} rows)
                      </span>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
            
            {selectedTable && (
              <div className="flex items-center gap-2 p-3 rounded-lg bg-success/10 border border-success/20 animate-fade-in">
                <CheckCircle2 className="w-4 h-4 text-success" />
                <p className="text-xs text-success">
                  Schema compatible • Primary key: <code className="font-mono bg-success/20 px-1 rounded">KEY</code>
                </p>
              </div>
            )}
          </div>
        ) : (
          <div className="space-y-3">
            <Label htmlFor="new-table" className="text-sm font-medium">
              New Table Name
            </Label>
            <Input
              id="new-table"
              placeholder="e.g., household_survey_2024"
              value={newTableName}
              onChange={(e) => setNewTableName(e.target.value)}
            />
            {selectedForm && (
              <p className="text-xs text-muted-foreground">
                Table will be created with {selectedForm.fields.length} columns matching your SurveyCTO form fields
              </p>
            )}
          </div>
        )}

        <div className="space-y-3">
          <Label className="text-sm font-medium flex items-center gap-2">
            <Key className="w-4 h-4" />
            Sync Mode
          </Label>
          <RadioGroup value={syncMode} onValueChange={(v) => setSyncMode(v as "insert" | "upsert")}>
            <div className="flex items-start gap-3 p-3 rounded-lg border border-border hover:border-primary/50 transition-colors">
              <RadioGroupItem value="upsert" id="upsert" className="mt-0.5" />
              <div>
                <Label htmlFor="upsert" className="font-medium cursor-pointer">
                  Upsert (Recommended)
                </Label>
                <p className="text-xs text-muted-foreground mt-0.5">
                  Insert new rows and update existing ones based on primary key
                </p>
              </div>
            </div>
            <div className="flex items-start gap-3 p-3 rounded-lg border border-border hover:border-primary/50 transition-colors">
              <RadioGroupItem value="insert" id="insert" className="mt-0.5" />
              <div>
                <Label htmlFor="insert" className="font-medium cursor-pointer">
                  Insert Only
                </Label>
                <p className="text-xs text-muted-foreground mt-0.5">
                  Only add new rows, skip duplicates
                </p>
              </div>
            </div>
          </RadioGroup>
        </div>

        <Button
          variant="gradient"
          size="lg"
          className="w-full"
          onClick={onContinue}
          disabled={!isValid}
        >
          Continue to Sync
          <ArrowRight className="w-4 h-4" />
        </Button>
      </CardContent>
    </Card>
  );
};

export default TableConfiguration;
