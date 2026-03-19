"""
一键运行完整验证流程
====================

Usage:
    python -m astrategy.research.run_full_validation
    python -m astrategy.research.run_full_validation --skip-debate
    python -m astrategy.research.run_full_validation --output-dir results/
    python -m astrategy.research.run_full_validation --max-events 5

流程：
  1. 加载历史事件
  2. 运行回测
  3. 计算指标
  4. Walk-Forward 验证
  5. 消融实验
  6. 生成审计报告 (Markdown)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("astrategy.research.full_validation")


def run_full_validation(
    events_file: str = "",
    max_events: int = 0,
    skip_debate: bool = True,
    output_dir: str = "",
    holding_days: int = 5,
    cost_bps: int = 40,
    wf_splits: int = 3,
    wf_train_ratio: float = 0.7,
    ablation_trials: int = 100,
) -> Dict[str, Any]:
    """运行完整验证流程。

    Returns
    -------
    dict
        包含所有验证结果和审计报告路径。
    """
    from astrategy.portfolio import (
        allocate_portfolio,
        build_execution_plan,
        simulate_portfolio,
    )
    from astrategy.research.backtest_engine import ShockBacktestEngine
    from astrategy.research.walk_forward import WalkForwardValidator
    from astrategy.research.ablation import AblationExperiment
    from astrategy.research.metrics import format_metrics_table

    t0 = time.time()

    # ── 1. 加载历史事件 ──
    logger.info("=" * 60)
    logger.info("步骤 1/6: 加载历史事件")
    logger.info("=" * 60)

    engine = ShockBacktestEngine(cost_bps=cost_bps, holding_days=holding_days)
    events = engine.load_events(path=events_file or None, max_events=max_events)
    logger.info("加载了 %d 个事件", len(events))

    # ── 2. 运行回测 ──
    logger.info("=" * 60)
    logger.info("步骤 2/6: 运行统一回测")
    logger.info("=" * 60)

    t_bt = time.time()
    results_df = engine.run(events, skip_debate=skip_debate)
    bt_elapsed = time.time() - t_bt
    logger.info("回测耗时: %.1fs, 产生 %d 个信号", bt_elapsed, len(results_df))

    # ── 3. 计算指标 ──
    logger.info("=" * 60)
    logger.info("步骤 3/6: 计算评价指标")
    logger.info("=" * 60)

    overall_metrics = engine.evaluate(results_df)
    overall_metrics["coverage"] = (
        round(results_df["event_id"].nunique() / len(events), 4)
        if (not results_df.empty and len(events) > 0)
        else 0.0
    )
    logger.info(
        "总体: Sharpe=%.2f, 胜率=%.1f%%, 信号=%d",
        overall_metrics.get("sharpe", 0),
        overall_metrics.get("win_rate", 0) * 100,
        overall_metrics.get("total_signals", 0),
    )

    # ── 4. Walk-Forward 验证 ──
    logger.info("=" * 60)
    logger.info("步骤 4/6: Walk-Forward 验证")
    logger.info("=" * 60)

    t_wf = time.time()
    wf_validator = WalkForwardValidator(n_splits=wf_splits, train_ratio=wf_train_ratio)
    wf_result = wf_validator.run(events, engine, skip_debate=skip_debate)
    wf_elapsed = time.time() - t_wf
    logger.info("Walk-Forward 耗时: %.1fs", wf_elapsed)

    # ── 5. 消融实验 ──
    logger.info("=" * 60)
    logger.info("步骤 5/6: 消融实验")
    logger.info("=" * 60)

    t_ab = time.time()
    ablation = AblationExperiment()
    ablation_result = ablation.run_all(
        events, engine, skip_debate=skip_debate, n_random_trials=ablation_trials,
    )
    ab_elapsed = time.time() - t_ab
    logger.info("消融实验耗时: %.1fs", ab_elapsed)

    if not results_df.empty:
        portfolio_signals = results_df.to_dict("records")
        portfolio = allocate_portfolio(portfolio_signals)
        portfolio_plan = build_execution_plan(portfolio)
        portfolio_sim = simulate_portfolio(
            portfolio_signals,
            holding_days=holding_days,
        )
    else:
        portfolio = {
            "positions": [],
            "defensive_positions": [],
            "gross_long_weight": 0.0,
            "defensive_weight": 0.0,
            "total_weight": 0.0,
            "num_positions": 0,
            "num_defensive": 0,
            "rotation_hints": [],
            "family_weights": [],
            "theme_weights": [],
            "event_type_weights": [],
            "constraint_stats": {},
        }
        portfolio_plan = "### 组合执行建议\n\n- 无有效信号，暂不建仓。"
        portfolio_sim = {
            "metrics": {},
            "daily_books": [],
            "avg_long_weight": 0.0,
            "avg_defensive_weight": 0.0,
            "avg_positions": 0.0,
            "avg_dynamic_scale": 0.0,
        }

    total_elapsed = time.time() - t0

    # ── 6. 生成审计报告 ──
    logger.info("=" * 60)
    logger.info("步骤 6/6: 生成审计报告")
    logger.info("=" * 60)

    report = _generate_audit_report(
        events=events,
        results_df=results_df,
        overall_metrics=overall_metrics,
        wf_result=wf_result,
        ablation_result=ablation_result,
        wf_validator=wf_validator,
        ablation_exp=ablation,
        portfolio=portfolio,
        portfolio_plan=portfolio_plan,
        portfolio_sim=portfolio_sim,
        total_elapsed=total_elapsed,
        bt_elapsed=bt_elapsed,
        wf_elapsed=wf_elapsed,
        ab_elapsed=ab_elapsed,
        holding_days=holding_days,
        cost_bps=cost_bps,
        skip_debate=skip_debate,
    )

    # 保存
    if output_dir:
        out_path = Path(output_dir)
    else:
        out_path = Path(__file__).resolve().parent.parent / ".data" / "reports"
    out_path.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d_%H%M")

    report_file = out_path / f"full_validation_{date_str}.md"
    report_file.write_text(report, encoding="utf-8")

    # 保存原始数据
    if not results_df.empty:
        signals_file = out_path / f"full_validation_signals_{date_str}.csv"
        results_df.to_csv(signals_file, index=False, encoding="utf-8-sig")
        logger.info("信号数据: %s", signals_file)

    metrics_file = out_path / f"full_validation_metrics_{date_str}.json"
    metrics_data = {
        "overall": overall_metrics,
        "walk_forward": {
            k: v for k, v in wf_result.items()
            if k != "splits" or True  # 保留全部
        },
        "ablation": ablation_result,
        "portfolio": {
            "snapshot": portfolio,
            "simulation": portfolio_sim,
        },
    }
    metrics_file.write_text(
        json.dumps(metrics_data, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    logger.info("审计报告: %s", report_file)
    logger.info("指标数据: %s", metrics_file)

    # 控制台摘要
    _print_summary(
        overall_metrics,
        wf_result,
        ablation_result,
        portfolio_sim,
        total_elapsed,
    )

    return {
        "report_path": str(report_file),
        "metrics_path": str(metrics_file),
        "overall_metrics": overall_metrics,
        "wf_result": wf_result,
        "ablation_result": ablation_result,
        "portfolio": portfolio,
        "portfolio_sim": portfolio_sim,
        "total_elapsed": total_elapsed,
    }


def _generate_audit_report(
    events,
    results_df,
    overall_metrics,
    wf_result,
    ablation_result,
    wf_validator,
    ablation_exp,
    portfolio,
    portfolio_plan,
    portfolio_sim,
    total_elapsed,
    bt_elapsed,
    wf_elapsed,
    ab_elapsed,
    holding_days,
    cost_bps,
    skip_debate,
) -> str:
    """生成完整审计报告 (Markdown)。"""
    from astrategy.research.metrics import format_metrics_table

    lines = [
        "# 冲击传播链路 -- 完整验证审计报告",
        "",
        f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**事件数**: {len(events)}",
        f"**信号数**: {len(results_df)}",
        f"**持有期**: {holding_days} 天",
        f"**交易成本**: {cost_bps} bps (往返)",
        f"**Agent辩论**: {'开启' if not skip_debate else '跳过'}",
        f"**总耗时**: {total_elapsed:.1f}s "
        f"(回测={bt_elapsed:.1f}s, WF={wf_elapsed:.1f}s, 消融={ab_elapsed:.1f}s)",
        "",
        "---",
        "",
    ]

    # ── 总体指标 ──
    lines.append(format_metrics_table(overall_metrics, title="总体指标"))
    lines.append("")

    # ── 按事件类型分组 ──
    by_event = overall_metrics.get("by_event_type", {})
    if by_event:
        lines.extend([
            "### 按事件类型分组",
            "",
            "| 事件类型 | 信号数 | Sharpe | 胜率 | 平均收益 |",
            "|---------|--------|--------|------|---------|",
        ])
        for etype in sorted(by_event.keys()):
            m = by_event[etype]
            lines.append(
                f"| {etype} "
                f"| {m.get('total_signals', 0)} "
                f"| {m.get('sharpe', 0):.2f} "
                f"| {m.get('win_rate', 0):.1%} "
                f"| {m.get('avg_return', 0):+.4f} |"
            )
        lines.append("")

    # ── 按跳数分组 ──
    by_hop = overall_metrics.get("by_hop", {})
    if by_hop:
        lines.extend([
            "### 按传播跳数分组",
            "",
            "| Hop | 信号数 | Sharpe | 胜率 | 平均收益 |",
            "|-----|--------|--------|------|---------|",
        ])
        for hop in sorted(by_hop.keys(), key=lambda x: int(x)):
            m = by_hop[hop]
            label = "源头" if str(hop) == "0" else f"{hop}跳"
            lines.append(
                f"| {label} "
                f"| {m.get('total_signals', 0)} "
                f"| {m.get('sharpe', 0):.2f} "
                f"| {m.get('win_rate', 0):.1%} "
                f"| {m.get('avg_return', 0):+.4f} |"
            )
        lines.append("")

    # ── 按是否反应分组 ──
    by_reacted = overall_metrics.get("by_reacted", {})
    if by_reacted:
        lines.extend([
            "### 按反应状态分组",
            "",
            "| 状态 | 信号数 | Sharpe | 胜率 | 平均收益 |",
            "|------|--------|--------|------|---------|",
        ])
        for key in sorted(by_reacted.keys()):
            m = by_reacted[key]
            label = "已反应" if key == "True" else "未反应"
            lines.append(
                f"| {label} "
                f"| {m.get('total_signals', 0)} "
                f"| {m.get('sharpe', 0):.2f} "
                f"| {m.get('win_rate', 0):.1%} "
                f"| {m.get('avg_return', 0):+.4f} |"
            )
        lines.append("")

    lines.extend([
        "### 研究诊断",
        "",
        f"- Coverage: {overall_metrics.get('coverage', 0):.1%}",
        f"- Breadth: {overall_metrics.get('breadth', 0):.4f}",
        f"- Confidence Spread: {overall_metrics.get('confidence_spread', 0):+.4f}",
        f"- Calibration: {overall_metrics.get('confidence_calibration', [])}",
        "",
    ])

    lines.append(portfolio_plan)
    lines.append("")

    portfolio_metrics = portfolio_sim.get("metrics", {})
    if portfolio_metrics:
        lines.append(format_metrics_table(portfolio_metrics, title="组合层回放"))
        lines.append("")
        lines.extend([
            "### 组合层诊断",
            "",
            f"- 日度建仓批次: {len(portfolio_sim.get('daily_books', []))}",
            f"- 平均长仓权重: {portfolio_sim.get('avg_long_weight', 0):.1%}",
            f"- 平均防守权重: {portfolio_sim.get('avg_defensive_weight', 0):.1%}",
            f"- 平均长仓持仓数: {portfolio_sim.get('avg_positions', 0):.2f}",
            f"- 平均动态仓位系数: {portfolio_sim.get('avg_dynamic_scale', 0):.2f}",
            "",
        ])

    # ── Walk-Forward ──
    lines.append("")
    lines.append(wf_validator.format_report(wf_result))
    lines.append("")

    # ── 消融实验 ──
    lines.append("")
    lines.append(ablation_exp.format_report(ablation_result))
    lines.append("")

    # ── 准入标准结论 ──
    lines.extend([
        "---",
        "",
        "## 准入标准总结",
        "",
    ])

    wf_passed = wf_result.get("passed", False)
    oos = wf_result.get("oos_aggregate", {})
    graph_ab = ablation_result.get("graph_ablation", {})
    react_ab = ablation_result.get("reaction_ablation", {})

    criteria_items = [
        ("WF-OOS Sharpe >= 0.8", oos.get("sharpe", 0) >= 0.8),
        ("WF-OOS 胜率 >= 55%", oos.get("win_rate", 0) >= 0.55),
        ("WF-OOS 信号数 >= 50", oos.get("total_signals", 0) >= 50),
        ("WF-OOS MaxDD <= 15%", abs(oos.get("max_drawdown", 0)) <= 0.15),
        ("图谱优于随机 (p < 0.05)", graph_ab.get("p_value_approx", 1) < 0.05),
        ("未反应 Sharpe > 已反应", react_ab.get("unreacted_advantage_sharpe", 0) > 0),
    ]

    lines.append("| 准入标准 | 结果 |")
    lines.append("|---------|------|")
    all_pass = True
    for label, passed in criteria_items:
        lines.append(f"| {label} | {'PASS' if passed else 'FAIL'} |")
        if not passed:
            all_pass = False

    lines.extend([
        "",
        f"**最终判定**: {'PASS -- 策略通过全部准入标准' if all_pass else 'FAIL -- 策略未通过部分准入标准，需继续优化'}",
    ])

    return "\n".join(lines)


def _print_summary(
    overall_metrics: Dict,
    wf_result: Dict,
    ablation_result: Dict,
    portfolio_sim: Dict,
    total_elapsed: float,
) -> None:
    """在控制台打印精简摘要。"""
    print()
    print("=" * 70)
    print("冲击传播链路 -- 完整验证结果")
    print("=" * 70)

    om = overall_metrics
    print(f"  总体: Sharpe={om.get('sharpe', 0):.2f} "
          f"胜率={om.get('win_rate', 0):.1%} "
          f"信号={om.get('total_signals', 0)} "
          f"MaxDD={om.get('max_drawdown', 0):.2%} "
          f"盈亏比={om.get('profit_factor', 0):.2f}")
    print(f"  研究诊断: Coverage={om.get('coverage', 0):.1%} "
          f"Breadth={om.get('breadth', 0):.4f} "
          f"ConfSpread={om.get('confidence_spread', 0):+.4f}")

    oos = wf_result.get("oos_aggregate", {})
    if oos:
        print(f"  WF-OOS: Sharpe={oos.get('sharpe', 0):.2f} "
              f"胜率={oos.get('win_rate', 0):.1%} "
              f"信号={oos.get('total_signals', 0)} "
              f"MaxDD={oos.get('max_drawdown', 0):.2%}")

    wf_passed = wf_result.get("passed", False)
    print(f"  WF准入: {'PASS' if wf_passed else 'FAIL'}")

    ga = ablation_result.get("graph_ablation", {})
    print(f"  图谱消融: 优势={ga.get('graph_advantage_sharpe', 0):+.2f} "
          f"p={ga.get('p_value_approx', 1):.4f}")

    ra = ablation_result.get("reaction_ablation", {})
    print(f"  反应过滤: 未反应优势={ra.get('unreacted_advantage_sharpe', 0):+.2f}")

    pm = portfolio_sim.get("metrics", {})
    if pm:
        print(f"  组合回放: Sharpe={pm.get('sharpe', 0):.2f} "
              f"胜率={pm.get('win_rate', 0):.1%} "
              f"批次={pm.get('total_signals', 0)} "
              f"MaxDD={pm.get('max_drawdown', 0):.2%}")
        print(f"  组合权重: 长仓={portfolio_sim.get('avg_long_weight', 0):.1%} "
              f"防守={portfolio_sim.get('avg_defensive_weight', 0):.1%} "
              f"平均持仓={portfolio_sim.get('avg_positions', 0):.2f} "
              f"动态系数={portfolio_sim.get('avg_dynamic_scale', 0):.2f}")

    print(f"  总耗时: {total_elapsed:.1f}s")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="冲击传播链路 -- 一键完整验证",
    )
    parser.add_argument(
        "--events-file", type=str, default="",
        help="事件文件路径(默认=.data/historical_events.json)",
    )
    parser.add_argument("--max-events", type=int, default=0, help="限制事件数(0=全部)")
    parser.add_argument("--skip-debate", action="store_true", help="跳过Agent辩论(快速)")
    parser.add_argument("--output-dir", type=str, default="", help="输出目录")
    parser.add_argument("--holding-days", type=int, default=5, help="持有期天数")
    parser.add_argument("--cost-bps", type=int, default=40, help="往返交易成本(bps)")
    parser.add_argument("--wf-splits", type=int, default=3, help="WF窗口数")
    parser.add_argument("--wf-train-ratio", type=float, default=0.7, help="WF训练占比")
    parser.add_argument("--ablation-trials", type=int, default=100, help="消融实验随机次数")
    args = parser.parse_args()

    run_full_validation(
        events_file=args.events_file,
        max_events=args.max_events,
        skip_debate=args.skip_debate,
        output_dir=args.output_dir,
        holding_days=args.holding_days,
        cost_bps=args.cost_bps,
        wf_splits=args.wf_splits,
        wf_train_ratio=args.wf_train_ratio,
        ablation_trials=args.ablation_trials,
    )


if __name__ == "__main__":
    main()
