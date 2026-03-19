"use client";
import { Suspense, useState, useCallback } from "react";
import { SideNav } from "./SideNav";
import { TopBar } from "./TopBar";

export function AppShell({ children }: { children: React.ReactNode }) {
  const [sideNavOpen, setSideNavOpen] = useState(false);

  const handleMenuClick = useCallback(() => setSideNavOpen(true), []);
  const handleClose = useCallback(() => setSideNavOpen(false), []);

  return (
    <Suspense fallback={null}>
      <div className="flex min-h-screen">
        <SideNav open={sideNavOpen} onClose={handleClose} />
        <div className="flex flex-col flex-1 min-w-0">
          <TopBar onMenuClick={handleMenuClick} />
          <main className="flex-1 bg-gray-50 dark:bg-gray-900 p-3 sm:p-4 lg:p-6 overflow-auto">
            {children}
          </main>
        </div>
      </div>
    </Suspense>
  );
}
