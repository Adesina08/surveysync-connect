import { Database } from "lucide-react";

const Header = () => {
  return (
    <header className="border-b border-border bg-card/80 backdrop-blur-sm sticky top-0 z-50">
      <div className="container mx-auto px-6 h-16 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-lg gradient-primary flex items-center justify-center shadow-card">
            <Database className="w-5 h-5 text-primary-foreground" />
          </div>
          <div>
            <h1 className="text-lg font-semibold text-foreground">DataSync</h1>
            <p className="text-xs text-muted-foreground -mt-0.5">SurveyCTO → PostgreSQL</p>
          </div>
        </div>
      </div>
    </header>
  );
};

export default Header;
