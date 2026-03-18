import { AlertTriangle, Info } from "lucide-react";

interface AlertBarProps {
  variant: "warning" | "danger" | "info";
  children: React.ReactNode;
}

const STYLES = {
  warning: "bg-amber-50 border-amber-200 text-amber-800",
  danger: "bg-red-50 border-red-200 text-red-800",
  info: "bg-blue-50 border-blue-200 text-blue-800",
};

export function AlertBar({ variant, children }: AlertBarProps) {
  const Icon = variant === "info" ? Info : AlertTriangle;
  return (
    <div className={`flex items-center gap-2 px-3 py-2 rounded-md border text-[12px] ${STYLES[variant]}`}>
      <Icon size={14} className="flex-shrink-0" />
      <div>{children}</div>
    </div>
  );
}
