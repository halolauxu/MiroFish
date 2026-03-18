"use client";
import { Menu } from "lucide-react";
import { GlobalFilterBar } from "@/components/filters/GlobalFilterBar";
import { ThemeToggle } from "@/components/theme";

export function TopBar({ onMenuClick }: { onMenuClick?: () => void }) {
  return (
    <header className="h-12 bg-white border-b border-gray-200 flex items-center justify-between px-4 flex-shrink-0">
      <div className="flex items-center gap-3">
        <button onClick={onMenuClick} className="lg:hidden text-gray-500 hover:text-gray-800">
          <Menu size={20} />
        </button>
        <div className="text-[11px] text-gray-400 font-mono hidden sm:block">
          冲击传播研究平台
        </div>
      </div>
      <div className="flex items-center gap-2">
        <GlobalFilterBar />
        <ThemeToggle />
      </div>
    </header>
  );
}
