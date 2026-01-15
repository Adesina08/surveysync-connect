import { useState } from "react";
import Header from "@/components/layout/Header";
import StepIndicator from "@/components/sync/StepIndicator";
import SurveyCTOLogin from "@/components/sync/SurveyCTOLogin";
import FormSelector from "@/components/sync/FormSelector";
import DatabaseConnection from "@/components/sync/DatabaseConnection";
import TableConfiguration from "@/components/sync/TableConfiguration";
import SyncExecution from "@/components/sync/SyncExecution";
import { SyncProvider } from "@/contexts/SyncContext";

const steps = [
  { id: 1, title: "Connect", description: "SurveyCTO login" },
  { id: 2, title: "Select Form", description: "Choose data source" },
  { id: 3, title: "Database", description: "PostgreSQL config" },
  { id: 4, title: "Table", description: "Target table setup" },
  { id: 5, title: "Sync", description: "Execute transfer" },
];

const IndexContent = () => {
  const [currentStep, setCurrentStep] = useState(1);

  const handleRestart = () => {
    setCurrentStep(1);
  };

  const renderStepContent = () => {
    switch (currentStep) {
      case 1:
        return <SurveyCTOLogin onSuccess={() => setCurrentStep(2)} />;
      case 2:
        return <FormSelector onSelect={() => setCurrentStep(3)} />;
      case 3:
        return <DatabaseConnection onSuccess={() => setCurrentStep(4)} />;
      case 4:
        return <TableConfiguration onContinue={() => setCurrentStep(5)} />;
      case 5:
        return (
          <SyncExecution
            onComplete={() => {}}
            onRestart={handleRestart}
          />
        );
      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Header />

      <main className="container mx-auto flex-1 px-6 py-8">
        {/* Hero Section for Step 1 */}
        {currentStep === 1 && (
          <div className="text-center mb-12 animate-fade-in">
            <h1 className="text-4xl font-bold mb-4">
              Sync Your Survey Data
              <span className="text-gradient block mt-1">Effortlessly</span>
            </h1>
            <p className="text-lg text-muted-foreground max-w-xl mx-auto">
              Connect your SurveyCTO forms directly to PostgreSQL. Just click and sync.
            </p>
          </div>
        )}

        {/* Step Indicator */}
        <div className="max-w-3xl mx-auto mb-12">
          <StepIndicator
            steps={steps}
            currentStep={currentStep}
            onStepClick={(stepId) => {
              if (stepId < currentStep) {
                setCurrentStep(stepId);
              }
            }}
          />
        </div>

        {/* Step Content */}
        <div className="mb-12">
          {renderStepContent()}
        </div>

      </main>

      {/* Footer */}
      <footer className="text-center text-sm text-muted-foreground py-8 border-t border-border mt-auto">
        <p>© 2026 InicioNG Tech Team</p>
      </footer>
    </div>
  );
};

const Index = () => {
  return (
    <SyncProvider>
      <IndexContent />
    </SyncProvider>
  );
};

export default Index;
