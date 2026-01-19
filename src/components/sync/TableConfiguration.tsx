import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from "@/components/ui/select";
import { cn } from "@/lib/utils";
import { useSyncContext } from "@/contexts/SyncContext";
import { getSchemas, getTables } from "@/api/postgres";
import { CheckCircle2, AlertTriangle, Table, Plus } from "lucide-react";

function safeNumber(value: unknown, fallback = 0): number {
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) ? n : fallback;
}

const TableConfiguration = ({ onNext, onBack }: { onNext: () => void; onBack: () => void }) => {
  const { state, setSelectedSchema, setSelectedTable, setCreateNewTable, setNewTableName } = useSyncContext();
  const { selectedSchema, selectedTable, createNewTable, newTableName, selectedForm } = state;

  const [schemas, setSchemas] = useState<{ name: string }[]>([]);
  const [tables, setTables] = useState<{ name: string; rowCount?: number }[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const loaded = await getSchemas();
        setSchemas(loaded);
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  useEffect(() => {
    (async () => {
      if (!selectedSchema) return;
      const loadedTables = await getTables(selectedSchema);
      setTables(loadedTables);
    })();
  }, [selectedSchema]);

  const canContinue = useMemo(() => {
    if (createNewTable) return Boolean(newTableName?.trim());
    return Boolean(selectedSchema && selectedTable);
  }, [createNewTable, newTableName, selectedSchema, selectedTable]);

  return (
    <div className="w-full max-w-2xl mx-auto space-y-6 animate-fade-in">
      <Card className="shadow-card border-border/50">
        <CardHeader className="pb-4">
          <CardTitle className="text-lg">Target Table Setup</CardTitle>
          <CardDescription>Select an existing table or create a new one</CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="space-y-3">
            <Label className="text-sm font-medium">Schema</Label>
            <Select value={selectedSchema || ""} onValueChange={(v) => setSelectedSchema(v)}>
              <SelectTrigger>
                <SelectValue placeholder={loading ? "Loading..." : "Choose a schema"} />
              </SelectTrigger>
              <SelectContent>
                {schemas.map((s) => (
                  <SelectItem key={s.name} value={s.name}>
                    {s.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="grid grid-cols-2 gap-4">
            <button
              type="button"
              className={cn(
                "p-4 rounded-xl border text-left transition-all",
                !createNewTable ? "border-primary bg-primary/5" : "border-border bg-background"
              )}
              onClick={() => setCreateNewTable(false)}
            >
              <Table className={cn("w-5 h-5 mb-2", !createNewTable ? "text-primary" : "text-muted-foreground")} />
              <p className="font-medium text-sm">Use Existing Table</p>
              <p className="text-xs text-muted-foreground mt-1">Pick a table from your database</p>
            </button>

            <button
              type="button"
              className={cn(
                "p-4 rounded-xl border text-left transition-all",
                createNewTable ? "border-primary bg-primary/5" : "border-border bg-background"
              )}
              onClick={() => setCreateNewTable(true)}
            >
              <Plus className={cn("w-5 h-5 mb-2", createNewTable ? "text-primary" : "text-muted-foreground")} />
              <p className="font-medium text-sm">Create New Table</p>
              <p className="text-xs text-muted-foreground mt-1">Auto-generate table from form schema</p>
            </button>
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
                          ({safeNumber((t as any).rowCount, 0).toLocaleString()} rows)
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
                    Schema compatible • Primary key:{" "}
                    <code className="font-mono bg-success/20 px-1 rounded">KEY</code>
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
                  Table will be created based on the fields in: <strong>{selectedForm.name}</strong>
                </p>
              )}
            </div>
          )}

          <div className="flex justify-between pt-2">
            <Button variant="outline" onClick={onBack}>
              Back
            </Button>
            <Button variant="gradient" onClick={onNext} disabled={!canContinue}>
              Continue
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default TableConfiguration;
