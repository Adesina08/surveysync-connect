import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import {
  Play,
  CheckCircle2,
  FileText,
  Database,
  ArrowRight,
  RefreshCw,
  Clock,
  Zap,
  BarChart3,
  AlertCircle
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useSyncContext } from "@/contexts/SyncContext";
import { startSyncJob, getSyncProgress } from "@/api/sync";
import type { SyncProgress as SyncProgressType } from "@/api/types";

interface SyncExecutionProps {
  onComplete: () => void;
  onRestart: () => void;
}

type SyncStatus = "ready" | "syncing" | "complete" | "failed";

function safeNumber(value: unknown, fallback = 0): number {
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function fmt(value: unknown): string {
  return safeNumber(value).toLocaleString();
}

const SyncExecution = ({ onComplete, onRestart }: SyncExecutionProps) => {
  const { state, reset } = useSyncContext();
  const { selectedForm, selectedSchema, selectedTable, createNewTable, newTableName, syncMode, sessionToken } = state;

  const [status, setStatus] = useState<SyncStatus>("ready");
  const [progress, setProgress] = useState<SyncProgressType | null>(null);
  const [duration, setDuration] = useState(0);

  const targetTable = createNewTable ? newTableName : selectedTable;
  const totalRows = safeNumber(selectedForm?.responses, 0);

  useEffect(() => {
    let interval: number | undefined;

    if (status === "syncing" && progress?.jobId != null) {
      interval = window.setInterval(async () => {
        // getSyncProgress accepts string in your api file, but backend returns number id.
        const updated = await getSyncProgress(String(progress.jobId));
        if (updated) {
          // normalize possible missing numeric fields
          const normalized: SyncProgressType = {
            ...updated,
            processedRecords: safeNumber((updated as any).processedRecords, 0),
            totalRecords: safeNumber((updated as any).totalRecords, 0),
            insertedRecords: safeNumber((updated as any).insertedRecords, 0),
            updatedRecords: safeNumber((updated as any).updatedRecords, 0),
            errors: Array.isArray((updated as any).errors) ? (updated as any).errors : [],
          };

          setProgress(normalized);

          if (normalized.status === "completed") {
            setStatus("complete");
            clearInterval(interval);
          } else if (normalized.status === "failed") {
            setStatus("failed");
            clearInterval(interval);
          }
        }
      }, 500);
    }

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [status, progress?.jobId]);

  useEffect(() => {
    let timer: number | undefined;

    if (status === "syncing") {
      timer = window.setInterval(() => {
        setDuration((prev) => prev + 1);
      }, 1000);
    }

    return () => {
      if (timer) clearInterval(timer);
    };
  }, [status]);

  const handleStartSync = async () => {
    if (!selectedForm || !selectedSchema || !targetTable) return;

    setStatus("syncing");
    setDuration(0);

    try {
      const initialProgress = await startSyncJob({
        formId: selectedForm.id,
        targetSchema: selectedSchema,
        targetTable: targetTable,
        syncMode,
        primaryKeyField: "KEY",
        createNewTable,
        sessionToken: sessionToken ?? undefined,
      });

      const normalized: SyncProgressType = {
        ...initialProgress,
        processedRecords: safeNumber((initialProgress as any).processedRecords, 0),
        totalRecords: safeNumber((initialProgress as any).totalRecords, 0),
        insertedRecords: safeNumber((initialProgress as any).insertedRecords, 0),
        updatedRecords: safeNumber((initialProgress as any).updatedRecords, 0),
        errors: Array.isArray((initialProgress as any).errors) ? (initialProgress as any).errors : [],
      };

      setProgress(normalized);

      // If backend instantly returns failed, flip UI to failed
      if (normalized.status === "failed") setStatus("failed");
      if (normalized.status === "completed") setStatus("complete");
    } catch (e: any) {
      // Prevent render crashes by setting a safe progress object
      setProgress({
        jobId: 0,
        status: "failed",
        processedRecords: 0,
        totalRecords: 0,
        insertedRecords: 0,
        updatedRecords: 0,
        errors: [
          {
            recordId: "n/a",
            message: e?.message || "Start sync failed",
          },
        ],
        startedAt: new Date().toISOString(),
        completedAt: new Date().toISOString(),
      });
      setStatus("failed");
    }
  };

  const handleRestart = () => {
    reset();
    onRestart();
  };

  const total = safeNumber(progress?.totalRecords, 0);
  const processed = safeNumber(progress?.processedRecords, 0);
  const progressPercent = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;

  const errorCount = progress?.errors ? progress.errors.length : 0;

  return (
    <div className="w-full max-w-2xl mx-auto space-y-6 animate-fade-in">
      {/* Summary Card */}
      <Card className="shadow-card border-border/50">
        <CardHeader className="pb-4">
          <CardTitle className="text-lg">Sync Configuration</CardTitle>
          <CardDescription>Review your sync settings before starting</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-4">
            <div className="flex items-center gap-3 p-3 rounded-lg bg-muted/50">
              <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
                <FileText className="w-5 h-5 text-primary" />
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Source</p>
                <p className="font-medium text-sm">{selectedForm?.name || "No form selected"}</p>
              </div>
            </div>
            <div className="flex items-center gap-3 p-3 rounded-lg bg-muted/50">
              <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
                <Database className="w-5 h-5 text-primary" />
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Target</p>
                <p className="font-medium text-sm">
                  {selectedSchema}.{targetTable}
                </p>
              </div>
            </div>
          </div>

          <div className="flex items-center justify-center gap-2 mt-4 text-sm text-muted-foreground">
            <span className="px-2 py-1 rounded bg-muted text-xs font-medium capitalize">
              {syncMode} Mode
            </span>
            <span>•</span>
            <span>{fmt(totalRows)} rows to sync</span>
          </div>
        </CardContent>
      </Card>

      {/* Progress Card */}
      <Card
        className={cn(
          "shadow-card border-border/50 transition-all duration-500",
          status === "syncing" && "border-primary/30",
          status === "complete" && "border-success/30 bg-success/5",
          status === "failed" && "border-destructive/30 bg-destructive/5"
        )}
      >
        <CardContent className="pt-6">
          {status === "ready" && (
            <div className="text-center py-8">
              <div className="w-20 h-20 rounded-2xl gradient-primary flex items-center justify-center mx-auto mb-6 shadow-card">
                <Zap className="w-10 h-10 text-primary-foreground" />
              </div>
              <h3 className="text-xl font-semibold mb-2">Ready to Sync</h3>
              <p className="text-muted-foreground mb-6 max-w-sm mx-auto">
                Click the button below to start synchronizing your SurveyCTO data to PostgreSQL
              </p>
              <Button variant="gradient" size="xl" onClick={handleStartSync}>
                <Play className="w-5 h-5" />
                Start Sync
              </Button>
            </div>
          )}

          {status === "syncing" && progress && (
            <div className="py-6">
              <div className="flex items-center justify-center mb-6">
                <div className="w-16 h-16 rounded-full border-4 border-primary/20 border-t-primary animate-spin" />
              </div>
              <h3 className="text-xl font-semibold text-center mb-2">Syncing Data...</h3>
              <p className="text-muted-foreground text-center mb-6">Please don't close this window</p>

              <div className="space-y-3">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Progress</span>
                  <span className="font-medium">{progressPercent}%</span>
                </div>

                <Progress value={progressPercent} className="h-2" />

                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Rows processed</span>
                  <span className="font-mono font-medium">
                    {fmt(progress.processedRecords)} / {fmt(progress.totalRecords)}
                  </span>
                </div>

                {errorCount > 0 && (
                  <div className="text-xs text-destructive">{errorCount} error(s) encountered</div>
                )}
              </div>
            </div>
          )}

          {status === "complete" && progress && (
            <div className="text-center py-6">
              <div className="w-20 h-20 rounded-2xl bg-success flex items-center justify-center mx-auto mb-6 shadow-card">
                <CheckCircle2 className="w-10 h-10 text-success-foreground" />
              </div>
              <h3 className="text-xl font-semibold mb-2 text-success">Sync Complete!</h3>
              <p className="text-muted-foreground mb-6">Your data has been successfully synchronized</p>

              <div className="grid grid-cols-4 gap-3 mb-6">
                <div className="p-3 rounded-lg bg-muted/50">
                  <BarChart3 className="w-4 h-4 text-primary mx-auto mb-1" />
                  <p className="text-xl font-bold">{fmt(progress.insertedRecords)}</p>
                  <p className="text-xs text-muted-foreground">Inserted</p>
                </div>
                <div className="p-3 rounded-lg bg-muted/50">
                  <RefreshCw className="w-4 h-4 text-primary mx-auto mb-1" />
                  <p className="text-xl font-bold">{fmt(progress.updatedRecords)}</p>
                  <p className="text-xs text-muted-foreground">Updated</p>
                </div>
                <div className="p-3 rounded-lg bg-muted/50">
                  <Clock className="w-4 h-4 text-primary mx-auto mb-1" />
                  <p className="text-xl font-bold">{duration}s</p>
                  <p className="text-xs text-muted-foreground">Duration</p>
                </div>
                <div className="p-3 rounded-lg bg-muted/50">
                  <AlertCircle className="w-4 h-4 text-muted-foreground mx-auto mb-1" />
                  <p className="text-xl font-bold">{errorCount}</p>
                  <p className="text-xs text-muted-foreground">Errors</p>
                </div>
              </div>

              <div className="flex gap-3 justify-center">
                <Button variant="outline" onClick={handleRestart}>
                  <RefreshCw className="w-4 h-4" />
                  New Sync
                </Button>
                <Button variant="gradient" onClick={onComplete}>
                  View in Database
                  <ArrowRight className="w-4 h-4" />
                </Button>
              </div>
            </div>
          )}

          {status === "failed" && progress && (
            <div className="text-center py-6">
              <div className="w-20 h-20 rounded-2xl bg-destructive flex items-center justify-center mx-auto mb-6 shadow-card">
                <AlertCircle className="w-10 h-10 text-destructive-foreground" />
              </div>
              <h3 className="text-xl font-semibold mb-2 text-destructive">Sync Failed</h3>
              <p className="text-muted-foreground mb-6">
                {(progress.errors?.[0]?.message as string) || "An unexpected error occurred."}
              </p>

              <div className="flex gap-3 justify-center">
                <Button variant="outline" onClick={handleRestart}>
                  <RefreshCw className="w-4 h-4" />
                  New Sync
                </Button>
                <Button variant="gradient" onClick={handleStartSync}>
                  <RefreshCw className="w-4 h-4" />
                  Try Again
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default SyncExecution;
