import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { FileText, Search, ArrowRight, Calendar, Hash, CheckCircle2, Columns } from "lucide-react";
import { cn } from "@/lib/utils";
import { useSyncContext } from "@/contexts/SyncContext";
import { formatDistanceToNow } from "date-fns";

interface FormSelectorProps {
  onSelect: (formId: string) => void;
}

const FormSelector = ({ onSelect }: FormSelectorProps) => {
  const { state, setSelectedForm } = useSyncContext();
  const [search, setSearch] = useState("");
  const [selectedFormId, setSelectedFormId] = useState<string | null>(null);

  const filteredForms = state.forms.filter((form) =>
    form.name.toLowerCase().includes(search.toLowerCase())
  );

  const handleSelect = (formId: string) => {
    setSelectedFormId(formId);
    const form = state.forms.find(f => f.id === formId);
    if (form) {
      setSelectedForm(form);
    }
  };

  const handleContinue = () => {
    if (selectedFormId) {
      onSelect(selectedFormId);
    }
  };

  const formatDate = (dateString: string) => {
    try {
      return formatDistanceToNow(new Date(dateString), { addSuffix: true });
    } catch {
      return dateString;
    }
  };

  return (
    <Card className="w-full max-w-2xl mx-auto shadow-card border-border/50 animate-fade-in">
      <CardHeader className="pb-4">
        <CardTitle className="text-xl">Select a Form</CardTitle>
        <CardDescription>
          Choose the SurveyCTO form you want to sync with PostgreSQL
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <Input
            placeholder="Search forms..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-10"
          />
        </div>

        {filteredForms.length === 0 ? (
          <div className="text-center py-8 text-muted-foreground">
            <FileText className="w-12 h-12 mx-auto mb-3 opacity-50" />
            <p>No forms found matching "{search}"</p>
          </div>
        ) : (
          <div className="space-y-2 max-h-[320px] overflow-y-auto pr-1">
            {filteredForms.map((form) => (
              <div
                key={form.id}
                onClick={() => handleSelect(form.id)}
                className={cn(
                  "p-4 rounded-lg border cursor-pointer transition-all duration-200 group",
                  selectedFormId === form.id
                    ? "border-primary bg-primary/5 shadow-card"
                    : "border-border hover:border-primary/50 hover:bg-muted/50"
                )}
              >
                <div className="flex items-start justify-between">
                  <div className="flex items-start gap-3">
                    <div
                      className={cn(
                        "w-10 h-10 rounded-lg flex items-center justify-center transition-colors",
                        selectedFormId === form.id
                          ? "bg-primary text-primary-foreground"
                          : "bg-muted text-muted-foreground group-hover:bg-primary/10 group-hover:text-primary"
                      )}
                    >
                      <FileText className="w-5 h-5" />
                    </div>
                    <div>
                      <h3 className="font-medium text-foreground">{form.name}</h3>
                      <p className="text-xs text-muted-foreground mb-1">v{form.version}</p>
                      <div className="flex items-center gap-4">
                        <span className="flex items-center gap-1 text-xs text-muted-foreground">
                          <Hash className="w-3 h-3" />
                          {form.responses.toLocaleString()} responses
                        </span>
                        <span className="flex items-center gap-1 text-xs text-muted-foreground">
                          <Columns className="w-3 h-3" />
                          {form.fields.length} fields
                        </span>
                        <span className="flex items-center gap-1 text-xs text-muted-foreground">
                          <Calendar className="w-3 h-3" />
                          {formatDate(form.lastUpdated)}
                        </span>
                      </div>
                    </div>
                  </div>
                  {selectedFormId === form.id && (
                    <CheckCircle2 className="w-5 h-5 text-primary" />
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        <Button
          variant="gradient"
          size="lg"
          className="w-full"
          disabled={!selectedFormId}
          onClick={handleContinue}
        >
          Continue with Selected Form
          <ArrowRight className="w-4 h-4" />
        </Button>
      </CardContent>
    </Card>
  );
};

export default FormSelector;
