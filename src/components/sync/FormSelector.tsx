import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { FileText, Search, ArrowRight, Calendar, Hash, CheckCircle2 } from "lucide-react";
import { cn } from "@/lib/utils";

interface Form {
  id: string;
  name: string;
  responses: number;
  lastUpdated: string;
}

const mockForms: Form[] = [
  { id: "1", name: "Household Survey 2024", responses: 2847, lastUpdated: "2 hours ago" },
  { id: "2", name: "Health Assessment Q4", responses: 1523, lastUpdated: "1 day ago" },
  { id: "3", name: "Education Baseline", responses: 4291, lastUpdated: "3 days ago" },
  { id: "4", name: "Agricultural Census", responses: 892, lastUpdated: "1 week ago" },
  { id: "5", name: "Water & Sanitation", responses: 3156, lastUpdated: "2 weeks ago" },
];

interface FormSelectorProps {
  onSelect: (formId: string) => void;
}

const FormSelector = ({ onSelect }: FormSelectorProps) => {
  const [search, setSearch] = useState("");
  const [selectedForm, setSelectedForm] = useState<string | null>(null);

  const filteredForms = mockForms.filter((form) =>
    form.name.toLowerCase().includes(search.toLowerCase())
  );

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

        <div className="space-y-2 max-h-[320px] overflow-y-auto pr-1">
          {filteredForms.map((form) => (
            <div
              key={form.id}
              onClick={() => setSelectedForm(form.id)}
              className={cn(
                "p-4 rounded-lg border cursor-pointer transition-all duration-200 group",
                selectedForm === form.id
                  ? "border-primary bg-primary/5 shadow-card"
                  : "border-border hover:border-primary/50 hover:bg-muted/50"
              )}
            >
              <div className="flex items-start justify-between">
                <div className="flex items-start gap-3">
                  <div
                    className={cn(
                      "w-10 h-10 rounded-lg flex items-center justify-center transition-colors",
                      selectedForm === form.id
                        ? "bg-primary text-primary-foreground"
                        : "bg-muted text-muted-foreground group-hover:bg-primary/10 group-hover:text-primary"
                    )}
                  >
                    <FileText className="w-5 h-5" />
                  </div>
                  <div>
                    <h3 className="font-medium text-foreground">{form.name}</h3>
                    <div className="flex items-center gap-4 mt-1">
                      <span className="flex items-center gap-1 text-xs text-muted-foreground">
                        <Hash className="w-3 h-3" />
                        {form.responses.toLocaleString()} responses
                      </span>
                      <span className="flex items-center gap-1 text-xs text-muted-foreground">
                        <Calendar className="w-3 h-3" />
                        {form.lastUpdated}
                      </span>
                    </div>
                  </div>
                </div>
                {selectedForm === form.id && (
                  <CheckCircle2 className="w-5 h-5 text-primary" />
                )}
              </div>
            </div>
          ))}
        </div>

        <Button
          variant="gradient"
          size="lg"
          className="w-full"
          disabled={!selectedForm}
          onClick={() => selectedForm && onSelect(selectedForm)}
        >
          Continue with Selected Form
          <ArrowRight className="w-4 h-4" />
        </Button>
      </CardContent>
    </Card>
  );
};

export default FormSelector;
