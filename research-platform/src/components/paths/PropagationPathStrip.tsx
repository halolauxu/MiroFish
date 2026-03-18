import { ArrowRight, AlertTriangle } from "lucide-react";
import type { PathSegment } from "@/types";

function PathNode({ code, name, isHighlight, isSource }: { code: string; name: string; isHighlight?: boolean; isSource?: boolean }) {
  return (
    <div
      className={`flex flex-col items-center px-3 py-1.5 rounded border text-center min-w-[80px] ${
        isHighlight
          ? "border-blue-400 bg-blue-50"
          : isSource
          ? "border-gray-300 bg-gray-50"
          : "border-gray-200 bg-white"
      }`}
    >
      <span className="text-[11px] font-mono text-gray-500">{code}</span>
      <span className="text-[12px] font-medium text-gray-800 truncate max-w-[100px]">{name}</span>
    </div>
  );
}

function PathEdge({ relation, shockWeight, isSuspicious, reason }: {
  relation: string;
  shockWeight: number;
  isSuspicious: boolean;
  reason?: string;
}) {
  return (
    <div className="flex flex-col items-center mx-1">
      <div className="flex items-center gap-0.5">
        <ArrowRight size={14} className={isSuspicious ? "text-red-400" : "text-gray-400"} />
      </div>
      <span className={`text-[9px] font-mono ${isSuspicious ? "text-red-500" : "text-gray-400"}`}>
        {relation.replace(/_/g, " ")}
      </span>
      <span className="text-[9px] text-gray-300">w={shockWeight.toFixed(2)}</span>
      {isSuspicious && (
        <span className="flex items-center gap-0.5 text-[8px] text-red-500 mt-0.5">
          <AlertTriangle size={8} />
          {reason || "可疑"}
        </span>
      )}
    </div>
  );
}

interface PropagationPathStripProps {
  segments: PathSegment[];
  sourceCode: string;
  sourceName: string;
  targetCode: string;
  isSourceOnly?: boolean;
}

export function PropagationPathStrip({ segments, sourceCode, sourceName, targetCode, isSourceOnly }: PropagationPathStripProps) {
  if (isSourceOnly || segments.length === 0) {
    return (
      <div className="flex items-center gap-2 py-2">
        <PathNode code={sourceCode} name={sourceName} isSource isHighlight />
        <span className="text-[10px] text-gray-400 italic ml-2">源头信号（hop=0）</span>
      </div>
    );
  }

  // Build chain of nodes
  const allNodes: { code: string; name: string }[] = [{ code: sourceCode, name: sourceName }];
  for (const seg of segments) {
    if (allNodes[allNodes.length - 1].code !== seg.from_code) {
      allNodes.push({ code: seg.from_code, name: seg.from_name });
    }
    allNodes.push({ code: seg.to_code, name: seg.to_name });
  }

  return (
    <div className="flex items-center gap-0.5 py-2 overflow-x-auto">
      {allNodes.map((node, i) => (
        <div key={`${node.code}-${i}`} className="flex items-center gap-0.5">
          {i > 0 && segments[i - 1] && (
            <PathEdge
              relation={segments[i - 1].relation}
              shockWeight={segments[i - 1].shock_weight_at_hop}
              isSuspicious={segments[i - 1].is_suspicious}
              reason={segments[i - 1].suspicion_reason}
            />
          )}
          <PathNode
            code={node.code}
            name={node.name}
            isSource={i === 0}
            isHighlight={node.code === targetCode}
          />
        </div>
      ))}
    </div>
  );
}
