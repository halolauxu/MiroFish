"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { LayoutDashboard, Network, X } from "lucide-react";

const NAV_ITEMS = [
  { href: "/", label: "研究总览", icon: LayoutDashboard },
  { href: "/graph", label: "图谱总览", icon: Network },
];

export function SideNav({ open, onClose }: { open?: boolean; onClose?: () => void }) {
  const pathname = usePathname();

  return (
    <>
      {/* Mobile overlay */}
      {open && (
        <div
          className="fixed inset-0 bg-black/40 z-40 lg:hidden"
          onClick={onClose}
        />
      )}

      <aside
        className={`
          fixed inset-y-0 left-0 z-50 w-[200px] bg-gray-900 text-gray-300 flex flex-col flex-shrink-0
          transform transition-transform duration-200 ease-in-out
          lg:relative lg:translate-x-0
          ${open ? "translate-x-0" : "-translate-x-full"}
        `}
      >
        <div className="h-12 flex items-center justify-between px-4 border-b border-gray-700">
          <div>
            <span className="text-sm font-bold text-white tracking-wide">MiroFish</span>
            <span className="text-[10px] ml-1.5 text-gray-500 font-mono">研究平台</span>
          </div>
          <button onClick={onClose} className="lg:hidden text-gray-400 hover:text-white">
            <X size={18} />
          </button>
        </div>
        <nav className="flex-1 py-2 overflow-y-auto">
          {NAV_ITEMS.map((item) => {
            const isActive = item.href === "/"
              ? pathname === "/"
              : pathname === item.href || pathname.startsWith(item.href + "/");
            const Icon = item.icon;
            return (
              <Link
                key={item.href}
                href={item.href}
                onClick={onClose}
                className={`flex items-center gap-2.5 px-4 py-2 text-[13px] transition-colors ${
                  isActive
                    ? "bg-gray-800 text-white border-l-2 border-blue-400"
                    : "hover:bg-gray-800/50 hover:text-white border-l-2 border-transparent"
                }`}
              >
                <Icon size={16} className={isActive ? "text-blue-400" : "text-gray-500"} />
                <span>{item.label}</span>
              </Link>
            );
          })}
        </nav>
        <div className="px-4 py-3 border-t border-gray-700 text-[10px] text-gray-600">
          v0.2.0 · Shock Pipeline
        </div>
      </aside>
    </>
  );
}
