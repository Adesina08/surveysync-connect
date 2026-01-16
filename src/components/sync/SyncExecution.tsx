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

const SyncExecution = ({ onComplete, onRestart }: SyncExecutionProps) => {
  const { state, reset } = useSyncContext();
  const { selectedForm, selectedSchema, selectedTable, createNewTable, newTableName, syncMode, sessionToken } = state;
  
  const [status, setStatus] = useState<SyncStatus>("ready");
  const [progress, setProgress] = useState<SyncProgressType | null>(null);
  const [duration, setDuration] = useState(0);

  const targetTable = createNewTable ? newTableName : selectedTable;
  const totalRows = selectedForm?.responses || 0;

  useEffect(() => {
    let interval: number | undefined;
    
    if (status === "syncing" && progress?.jobId) {
      interval = window.setInterval(async () => {
        const updated = await getSyncProgress(progress.jobId);
        if (updated) {
          setProgress(updated);
          
          if (updated.status === 'completed') {
            setStatus("complete");
            clearInterval(interval);
          } else if (updated.status === 'failed') {
            setStatus("failed");
            clearInterval(interval);
          }
        }
      }, 200);
    }

    return () => {
      if (interval) clearInterval(interval);
    };
  }, [status, progress?.jobId]);

  // Duration timer
  useEffect(() => {
    let timer: number | undefined;
    
    if (status === "syncing") {
      timer = window.setInterval(() => {
        setDuration(prev => prev + 1);
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
    
    const initialProgress = await startSyncJob({
      formId: selectedForm.id,
      targetSchema: selectedSchema,
      targetTable: targetTable,
      syncMode,
      primaryKeyField: 'KEY',
      createNewTable,
      sessionToken: sessionToken ?? undefined,
    });
    
    setProgress(initialProgress);
  };

  const handleRestart = () => {
    reset();
    onRestart();
  };

  const progressPercent = progress?.totalRecords 
    ? Math.round((progress.processedRecords / progress.totalRecords) * 100) 
    : 0;

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
                <p className="font-medium text-sm">{selectedForm?.name || 'No form selected'}</p>
              </div>
            </div>
            <div className="flex items-center gap-3 p-3 rounded-lg bg-muted/50">
              <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
                <Database className="w-5 h-5 text-primary" />
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Target</p>
                <p className="font-medium text-sm">{selectedSchema}.{targetTable}</p>
              </div>
            </div>
          </div>
          <div className="flex items-center justify-center gap-2 mt-4 text-sm text-muted-foreground">
            <span className="px-2 py-1 rounded bg-muted text-xs font-medium capitalize">
              {syncMode} Mode
            </span>
            <span>•</span>
            <span>{totalRows.toLocaleString()} rows to sync</span>
          </div>
        </CardContent>
      </Card>

      {/* Progress Card */}
      <Card className={cn(
        "shadow-card border-border/50 transition-all duration-500",
        status === "syncing" && "border-primary/30",
        status === "complete" && "border-success/30 bg-success/5",
        status === "failed" && "border-destructive/30 bg-destructive/5"
      )}>
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
              <p className="text-muted-foreground text-center mb-6">
                Please don't close this window
              </p>
              <div className="space-y-3">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Progress</span>
                  <span className="font-medium">{progressPercent}%</span>
                </div>
                <Progress value={progressPercent} className="h-2" />
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Rows processed</span>
                  <span className="font-mono font-medium">
                    {progress.processedRecords.toLocaleString()} / {progress.totalRecords.toLocaleString()}
                  </span>
                </div>
                {progress.errors.length > 0 && (
                  <div className="text-xs text-destructive">
                    {progress.errors.length} error(s) encountered
                  </div>
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
              <p className="text-muted-foreground mb-6">
                Your data has been successfully synchronized
              </p>

              <div className="grid grid-cols-4 gap-3 mb-6">
                <div className="p-3 rounded-lg bg-muted/50">
                  <BarChart3 className="w-4 h-4 text-primary mx-auto mb-1" />
                  <p className="text-xl font-bold">{progress.insertedRecords.toLocaleString()}</p>
                  <p className="text-xs text-muted-foreground">Inserted</p>
                </div>
                <div className="p-3 rounded-lg bg-muted/50">
                  <RefreshCw className="w-4 h-4 text-primary mx-auto mb-1" />
                  <p className="text-xl font-bold">{progress.updatedRecords.toLocaleString()}</p>
                  <p className="text-xs text-muted-foreground">Updated</p>
                </div>
                <div className="p-3 rounded-lg bg-muted/50">
                  <Clock className="w-4 h-4 text-primary mx-auto mb-1" />
                  <p className="text-xl font-bold">{duration}s</p>
                  <p className="text-xs text-muted-foreground">Duration</p>
                </div>
                <div className="p-3 rounded-lg bg-muted/50">
                  <AlertCircle className="w-4 h-4 text-muted-foreground mx-auto mb-1" />
                  <p className="text-xl font-bold">{progress.errors.length}</p>
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

          {status === "failed" && (
            <div className="text-center py-6">
              <div className="w-20 h-20 rounded-2xl bg-destructive flex items-center justify-center mx-auto mb-6 shadow-card">
                <AlertCircle className="w-10 h-10 text-destructive-foreground" />
              </div>
              <h3 className="text-xl font-semibold mb-2 text-destructive">Sync Failed</h3>
              <p className="text-muted-foreground mb-6">
                {progress?.errors[0]?.message || 'An error occurred during synchronization'}
              </p>
              <Button variant="outline" onClick={handleRestart}>
                <RefreshCw className="w-4 h-4" />
                Try Again
              </Button>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default SyncExecution;
