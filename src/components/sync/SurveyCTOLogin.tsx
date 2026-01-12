import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Lock, Server, User, ArrowRight, Shield, AlertCircle } from "lucide-react";
import { authenticateSurveyCTO } from "@/api/surveycto";
import { useSyncContext } from "@/contexts/SyncContext";

interface SurveyCTOLoginProps {
  onSuccess: () => void;
}

const SurveyCTOLogin = ({ onSuccess }: SurveyCTOLoginProps) => {
  const { setSessionToken, setForms } = useSyncContext();
  const [serverName, setServerName] = useState("");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError(null);

    const response = await authenticateSurveyCTO({
      serverName: serverName.trim(),
      username: username.trim(),
      password,
    });

    setIsLoading(false);

    if (response.success && response.sessionToken && response.forms) {
      setSessionToken(response.sessionToken);
      setForms(response.forms);
      onSuccess();
    } else {
      setError(response.error || 'Authentication failed');
    }
  };

  return (
    <Card className="w-full max-w-md mx-auto shadow-card border-border/50 animate-fade-in">
      <CardHeader className="text-center pb-2">
        <div className="w-14 h-14 rounded-xl gradient-primary flex items-center justify-center mx-auto mb-4 shadow-card">
          <Server className="w-7 h-7 text-primary-foreground" />
        </div>
        <CardTitle className="text-xl">Connect to SurveyCTO</CardTitle>
        <CardDescription className="text-muted-foreground">
          Enter your SurveyCTO server credentials to get started
        </CardDescription>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="server" className="text-sm font-medium">
              Server Name
            </Label>
            <div className="relative">
              <Server className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                id="server"
                placeholder="your-server"
                value={serverName}
                onChange={(e) => setServerName(e.target.value)}
                className="pl-10"
                required
              />
              <span className="absolute right-3 top-1/2 -translate-y-1/2 text-sm text-muted-foreground">
                .surveycto.com
              </span>
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="username" className="text-sm font-medium">
              Username
            </Label>
            <div className="relative">
              <User className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                id="username"
                placeholder="Enter your username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="pl-10"
                required
              />
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="password" className="text-sm font-medium">
              Password
            </Label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                id="password"
                type="password"
                placeholder="Enter your password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="pl-10"
                required
              />
            </div>
          </div>

          {error && (
            <div className="flex items-center gap-2 p-3 rounded-lg bg-destructive/10 border border-destructive/20">
              <AlertCircle className="w-4 h-4 text-destructive flex-shrink-0" />
              <p className="text-sm text-destructive">{error}</p>
            </div>
          )}

          <div className="flex items-center gap-2 p-3 rounded-lg bg-muted/50 border border-border/50">
            <Shield className="w-4 h-4 text-success flex-shrink-0" />
            <p className="text-xs text-muted-foreground">
              Your credentials are encrypted and never stored on our servers
            </p>
          </div>

          <Button
            type="submit"
            variant="gradient"
            size="lg"
            className="w-full"
            disabled={isLoading}
          >
            {isLoading ? (
              <span className="flex items-center gap-2">
                <span className="w-4 h-4 border-2 border-primary-foreground/30 border-t-primary-foreground rounded-full animate-spin" />
                Connecting...
              </span>
            ) : (
              <span className="flex items-center gap-2">
                Connect to SurveyCTO
                <ArrowRight className="w-4 h-4" />
              </span>
            )}
          </Button>
        </form>
      </CardContent>
    </Card>
  );
};

export default SurveyCTOLogin;
