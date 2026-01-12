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
  BarChart3
} from "lucide-react";
import { cn } from "@/lib/utils";

interface SyncExecutionProps {
  onComplete: () => void;
  onRestart: () => void;
}

type SyncStatus = "ready" | "syncing" | "complete";

const SyncExecution = ({ onComplete, onRestart }: SyncExecutionProps) => {
  const [status, setStatus] = useState<SyncStatus>("ready");
  const [progress, setProgress] = useState(0);
  const [rowsProcessed, setRowsProcessed] = useState(0);
  const totalRows = 2847;

  useEffect(() => {
    if (status === "syncing") {
      const interval = setInterval(() => {
        setProgress((prev) => {
          const next = prev + Math.random() * 15;
          if (next >= 100) {
            clearInterval(interval);
            setStatus("complete");
            setRowsProcessed(totalRows);
            return 100;
          }
          setRowsProcessed(Math.floor((next / 100) * totalRows));
          return next;
        });
      }, 300);
      return () => clearInterval(interval);
    }
  }, [status]);

  const handleStartSync = () => {
    setStatus("syncing");
    setProgress(0);
    setRowsProcessed(0);
  };

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
                <p className="font-medium text-sm">Household Survey 2024</p>
              </div>
            </div>
            <div className="flex items-center gap-3 p-3 rounded-lg bg-muted/50">
              <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
                <Database className="w-5 h-5 text-primary" />
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Target</p>
                <p className="font-medium text-sm">public.survey_responses</p>
              </div>
            </div>
          </div>
          <div className="flex items-center justify-center gap-2 mt-4 text-sm text-muted-foreground">
            <span className="px-2 py-1 rounded bg-muted text-xs font-medium">Upsert Mode</span>
            <span>•</span>
            <span>{totalRows.toLocaleString()} rows to sync</span>
          </div>
        </CardContent>
      </Card>

      {/* Progress Card */}
      <Card className={cn(
        "shadow-card border-border/50 transition-all duration-500",
        status === "syncing" && "border-primary/30",
        status === "complete" && "border-success/30 bg-success/5"
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

          {status === "syncing" && (
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
                  <span className="font-medium">{Math.round(progress)}%</span>
                </div>
                <Progress value={progress} className="h-2" />
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Rows processed</span>
                  <span className="font-mono font-medium">
                    {rowsProcessed.toLocaleString()} / {totalRows.toLocaleString()}
                  </span>
                </div>
              </div>
            </div>
          )}

          {status === "complete" && (
            <div className="text-center py-6">
              <div className="w-20 h-20 rounded-2xl bg-success flex items-center justify-center mx-auto mb-6 shadow-card">
                <CheckCircle2 className="w-10 h-10 text-success-foreground" />
              </div>
              <h3 className="text-xl font-semibold mb-2 text-success">Sync Complete!</h3>
              <p className="text-muted-foreground mb-6">
                Your data has been successfully synchronized
              </p>

              <div className="grid grid-cols-3 gap-4 mb-6">
                <div className="p-4 rounded-lg bg-muted/50">
                  <BarChart3 className="w-5 h-5 text-primary mx-auto mb-2" />
                  <p className="text-2xl font-bold">{totalRows.toLocaleString()}</p>
                  <p className="text-xs text-muted-foreground">Rows Synced</p>
                </div>
                <div className="p-4 rounded-lg bg-muted/50">
                  <Clock className="w-5 h-5 text-primary mx-auto mb-2" />
                  <p className="text-2xl font-bold">12s</p>
                  <p className="text-xs text-muted-foreground">Duration</p>
                </div>
                <div className="p-4 rounded-lg bg-muted/50">
                  <CheckCircle2 className="w-5 h-5 text-success mx-auto mb-2" />
                  <p className="text-2xl font-bold">0</p>
                  <p className="text-xs text-muted-foreground">Errors</p>
                </div>
              </div>

              <div className="flex gap-3 justify-center">
                <Button variant="outline" onClick={onRestart}>
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
        </CardContent>
      </Card>
    </div>
  );
};

export default SyncExecution;
