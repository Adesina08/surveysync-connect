import { useState } from "react";
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
import { Database, Server, User, Lock, Hash, ArrowRight, CheckCircle2, AlertCircle } from "lucide-react";
import { connectPostgres } from "@/api/postgres";
import { useSyncContext } from "@/contexts/SyncContext";
import type { PostgresCredentials } from "@/api/types";

interface DatabaseConnectionProps {
  onSuccess: () => void;
}

const DatabaseConnection = ({ onSuccess }: DatabaseConnectionProps) => {
  const { setSchemas } = useSyncContext();
  const [host, setHost] = useState("");
  const [port, setPort] = useState("5432");
  const [database, setDatabase] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [sslMode, setSslMode] = useState<PostgresCredentials['sslMode']>("require");
  const [isConnecting, setIsConnecting] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState<"idle" | "success" | "error">("idle");
  const [error, setError] = useState<string | null>(null);

  const handleConnect = async () => {
    setIsConnecting(true);
    setConnectionStatus("idle");
    setError(null);

    const response = await connectPostgres({
      host: host.trim(),
      port: parseInt(port, 10) || 5432,
      database: database.trim(),
      username: username.trim(),
      password,
      sslMode,
    });

    setIsConnecting(false);

    if (response.success && response.schemas) {
      setConnectionStatus("success");
      setSchemas(response.schemas);
      
      // Auto-proceed after success
      setTimeout(() => {
        onSuccess();
      }, 1000);
    } else {
      setConnectionStatus("error");
      setError(response.error || 'Connection failed');
    }
  };

  return (
    <Card className="w-full max-w-lg mx-auto shadow-card border-border/50 animate-fade-in">
      <CardHeader className="text-center pb-2">
        <div className="w-14 h-14 rounded-xl gradient-primary flex items-center justify-center mx-auto mb-4 shadow-card">
          <Database className="w-7 h-7 text-primary-foreground" />
        </div>
        <CardTitle className="text-xl">Connect to PostgreSQL</CardTitle>
        <CardDescription className="text-muted-foreground">
          Enter your database connection details
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-3 gap-3">
          <div className="col-span-2 space-y-2">
            <Label htmlFor="host" className="text-sm font-medium">
              Host
            </Label>
            <div className="relative">
              <Server className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                id="host"
                placeholder="localhost or IP address"
                value={host}
                onChange={(e) => setHost(e.target.value)}
                className="pl-10"
              />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="port" className="text-sm font-medium">
              Port
            </Label>
            <div className="relative">
              <Hash className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                id="port"
                placeholder="5432"
                value={port}
                onChange={(e) => setPort(e.target.value)}
                className="pl-10"
              />
            </div>
          </div>
        </div>

        <div className="space-y-2">
          <Label htmlFor="database" className="text-sm font-medium">
            Database Name
          </Label>
          <div className="relative">
            <Database className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
            <Input
              id="database"
              placeholder="Enter database name"
              value={database}
              onChange={(e) => setDatabase(e.target.value)}
              className="pl-10"
            />
          </div>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div className="space-y-2">
            <Label htmlFor="db-username" className="text-sm font-medium">
              Username
            </Label>
            <div className="relative">
              <User className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                id="db-username"
                placeholder="postgres"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="pl-10"
              />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="db-password" className="text-sm font-medium">
              Password
            </Label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                id="db-password"
                type="password"
                placeholder="••••••••"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="pl-10"
              />
            </div>
          </div>
        </div>

        <div className="space-y-2">
          <Label className="text-sm font-medium">SSL Mode</Label>
          <Select value={sslMode} onValueChange={(v) => setSslMode(v as PostgresCredentials['sslMode'])}>
            <SelectTrigger>
              <SelectValue placeholder="Select SSL mode" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="disable">Disable</SelectItem>
              <SelectItem value="prefer">Prefer</SelectItem>
              <SelectItem value="require">Require</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {connectionStatus === "success" && (
          <div className="flex items-center gap-2 p-3 rounded-lg bg-success/10 border border-success/20">
            <CheckCircle2 className="w-5 h-5 text-success" />
            <p className="text-sm text-success font-medium">
              Connection successful! Proceeding...
            </p>
          </div>
        )}

        {connectionStatus === "error" && error && (
          <div className="flex items-center gap-2 p-3 rounded-lg bg-destructive/10 border border-destructive/20">
            <AlertCircle className="w-5 h-5 text-destructive flex-shrink-0" />
            <p className="text-sm text-destructive">{error}</p>
          </div>
        )}

        <Button
          variant="gradient"
          size="lg"
          className="w-full"
          onClick={handleConnect}
          disabled={isConnecting || connectionStatus === "success"}
        >
          {isConnecting ? (
            <span className="flex items-center gap-2">
              <span className="w-4 h-4 border-2 border-primary-foreground/30 border-t-primary-foreground rounded-full animate-spin" />
              Testing Connection...
            </span>
          ) : connectionStatus === "success" ? (
            <span className="flex items-center gap-2">
              <CheckCircle2 className="w-4 h-4" />
              Connected
            </span>
          ) : (
            <span className="flex items-center gap-2">
              Test & Connect
              <ArrowRight className="w-4 h-4" />
            </span>
          )}
        </Button>
      </CardContent>
    </Card>
  );
};

export default DatabaseConnection;
