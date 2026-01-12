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

interface DatabaseConnectionProps {
  onSuccess: () => void;
}

const DatabaseConnection = ({ onSuccess }: DatabaseConnectionProps) => {
  const [host, setHost] = useState("");
  const [port, setPort] = useState("5432");
  const [database, setDatabase] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [isConnecting, setIsConnecting] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState<"idle" | "success" | "error">("idle");

  const handleConnect = async () => {
    setIsConnecting(true);
    setConnectionStatus("idle");
    
    // Simulate connection test
    await new Promise((resolve) => setTimeout(resolve, 2000));
    
    setIsConnecting(false);
    setConnectionStatus("success");
    
    // Auto-proceed after success
    setTimeout(() => {
      onSuccess();
    }, 1000);
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
          <Select defaultValue="require">
            <SelectTrigger>
              <SelectValue placeholder="Select SSL mode" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="disable">Disable</SelectItem>
              <SelectItem value="require">Require</SelectItem>
              <SelectItem value="verify-ca">Verify CA</SelectItem>
              <SelectItem value="verify-full">Verify Full</SelectItem>
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

        {connectionStatus === "error" && (
          <div className="flex items-center gap-2 p-3 rounded-lg bg-destructive/10 border border-destructive/20">
            <AlertCircle className="w-5 h-5 text-destructive" />
            <p className="text-sm text-destructive font-medium">
              Connection failed. Please check your credentials.
            </p>
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
