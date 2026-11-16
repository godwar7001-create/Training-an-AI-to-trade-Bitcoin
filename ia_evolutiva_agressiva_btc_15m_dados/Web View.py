#!/usr/bin/env python3
# Monitor independente, somente leitura, da IA Evolutiva Agressiva BTC 15m.
# Exibe um painel web na rede local; não altera o banco nem envia ordens.

from __future__ import annotations

import argparse
import ipaddress
import json
import secrets
import socket
import sqlite3
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlsplit


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DB = SCRIPT_DIR / "memoria_ia_evolutiva_agressiva.sqlite3"
POLL_SECONDS = 3


def parse_value(raw, default=None):
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


def format_time_ms(value, include_utc=True):
    if value is None:
        return None
    try:
        dt = datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)
        local = dt.astimezone().strftime("%d/%m/%Y %H:%M:%S")
        utc = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        return f"{local} (horário deste PC)" + (f" | {utc}" if include_utc else "")
    except (ValueError, TypeError, OverflowError, OSError):
        return str(value)


def format_iso_time(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%d/%m/%Y %H:%M:%S") + " (horário deste PC)"
    except (ValueError, TypeError, OverflowError, OSError):
        return str(value)


def read_only_connection(db_path):
    encoded = quote(db_path.resolve().as_posix(), safe="/:")
    conn = sqlite3.connect(f"file:{encoded}?mode=ro", uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def get_status(db_path):
    if not db_path.exists():
        raise FileNotFoundError("Não encontrei o banco SQLite ao lado deste script.")

    conn = read_only_connection(db_path)
    try:
        meta = {}
        if table_exists(conn, "meta"):
            meta = {
                row["key"]: parse_value(row["value"])
                for row in conn.execute("SELECT key, value FROM meta")
            }

        config = meta.get("configuracao_inicial") or {}
        if not isinstance(config, dict):
            config = {}

        latest_snapshot = None
        snapshot_count = 0
        if table_exists(conn, "snapshots"):
            snapshot_count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            row = conn.execute(
                "SELECT timestamp_ms, candle_open_time, open, high, low, price, "
                "volume, websocket_open FROM snapshots "
                "ORDER BY timestamp_ms DESC LIMIT 1"
            ).fetchone()
            latest_snapshot = dict(row) if row else None

        price = None
        last_snapshot_ms = None
        age_seconds = None
        if latest_snapshot:
            price = latest_snapshot.get("price")
            last_snapshot_ms = latest_snapshot.get("timestamp_ms")
            age_seconds = max(0.0, time.time() - float(last_snapshot_ms) / 1000)

        agents = []
        if table_exists(conn, "agents"):
            for row in conn.execute(
                "SELECT agent_id, generation, parent_id, genome_json, state_json, "
                "fitness, status FROM agents"
            ):
                try:
                    state = json.loads(row["state_json"] or "{}")
                    genome = json.loads(row["genome_json"] or "{}")
                except (TypeError, ValueError):
                    continue
                cash = float(state.get("cash") or 0)
                btc = float(state.get("btc") or 0)
                marked_equity = cash + btc * float(price or state.get("last_price") or 0)
                agents.append({
                    "id": row["agent_id"],
                    "generation": row["generation"],
                    "status": row["status"] or state.get("status", "—"),
                    "action": state.get("last_action", "—"),
                    "equity": marked_equity,
                    "fitness": float(row["fitness"] or 0),
                    "probability_up": state.get("last_probability"),
                    "threshold": genome.get("entry_threshold", state.get("entry_threshold")),
                    "risk": genome.get("risk_per_trade", state.get("risk_per_trade")),
                    "trade_count": int(state.get("trade_count") or 0),
                    "wins": int(state.get("trade_wins") or 0),
                    "losses": int(state.get("trade_losses") or 0),
                    "btc": btc,
                    "entry_time_ms": state.get("entry_time_ms"),
                    "entry_price": state.get("entry_price"),
                    "prediction_correct": int(state.get("prediction_correct") or 0),
                    "prediction_error": int(state.get("prediction_error") or 0),
                })

        starting_per_agent = float(config.get("banca_por_ia_usdt") or 100.0)
        population = int(meta.get("population_size") or config.get("population_size") or len(agents) or 0)
        starting_total = starting_per_agent * population
        current_equity = sum(a["equity"] for a in agents)
        total_cash = sum(
            float(json.loads(r["state_json"] or "{}").get("cash") or 0)
            for r in conn.execute("SELECT state_json FROM agents")
        ) if agents else 0.0
        open_agents = [a for a in agents if a["btc"] > 0]
        open_btc = sum(a["btc"] for a in open_agents)
        open_notional = open_btc * float(price or 0)

        trade_summary = {
            "count": 0, "wins": 0, "losses": 0, "net": 0.0,
            "fees": 0.0, "gross": 0.0, "win_rate": None,
        }
        if table_exists(conn, "trades"):
            row = conn.execute(
                "SELECT COUNT(*) AS n, "
                "SUM(CASE WHEN net_pnl_usdt > 0 THEN 1 ELSE 0 END) AS wins, "
                "SUM(CASE WHEN net_pnl_usdt <= 0 THEN 1 ELSE 0 END) AS losses, "
                "COALESCE(SUM(net_pnl_usdt),0) AS net, "
                "COALESCE(SUM(entry_fee_usdt + exit_fee_usdt),0) AS fees, "
                "COALESCE(SUM(gross_pnl_usdt),0) AS gross "
                "FROM trades"
            ).fetchone()
            trade_summary.update({
                "count": int(row["n"] or 0),
                "wins": int(row["wins"] or 0),
                "losses": int(row["losses"] or 0),
                "net": float(row["net"] or 0),
                "fees": float(row["fees"] or 0),
                "gross": float(row["gross"] or 0),
            })
            if trade_summary["count"]:
                trade_summary["win_rate"] = (
                    100.0 * trade_summary["wins"] / trade_summary["count"]
                )

        correct = sum(a["prediction_correct"] for a in agents)
        incorrect = sum(a["prediction_error"] for a in agents)
        prediction_count = correct + incorrect
        prediction_accuracy = (100.0 * correct / prediction_count) if prediction_count else None

        candle_count = 0
        if table_exists(conn, "candles"):
            candle_count = conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0]

        decision_count = 0
        if table_exists(conn, "decisions"):
            decision_count = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]

        generation = meta.get("current_generation", 1)
        generation_candles = int(meta.get("generation_candles") or 0)
        interval = str(config.get("intervalo") or "15m")
        try:
            if interval.endswith("m"):
                candle_minutes = int(interval[:-1])
            elif interval.endswith("h"):
                candle_minutes = int(interval[:-1]) * 60
            else:
                candle_minutes = 15
        except ValueError:
            candle_minutes = 15
        generation_days = int(config.get("geracao_dias") or 7)
        candles_per_generation = max(1, generation_days * 24 * 60 // max(1, candle_minutes))

        metrics = meta.get("training_metrics") or {}
        if not isinstance(metrics, dict):
            metrics = {}

        best_agent = meta.get("global_best_agent_id")
        last_signal = meta.get("last_signal") or "Ainda não há sinal salvo"
        last_signal_time = meta.get("last_signal_time")
        action_counts = {}
        for a in agents:
            action_counts[a["action"]] = action_counts.get(a["action"], 0) + 1

        top_agents = sorted(
            agents,
            key=lambda a: (a["fitness"], a["equity"]),
            reverse=True,
        )[:10]

        recent_trades = []
        if table_exists(conn, "trades"):
            for row in conn.execute(
                "SELECT agent_id, exit_time_ms, entry_price, exit_price, "
                "net_pnl_usdt, exit_reason FROM trades "
                "ORDER BY exit_time_ms DESC LIMIT 8"
            ):
                trade = dict(row)
                trade["exit_time_display"] = format_time_ms(trade.get("exit_time_ms"), False)
                recent_trades.append(trade)

        # O gráfico carrega até sete dias de candles e movimentos das IAs.
        chart_bars = []
        if table_exists(conn, "candles"):
            rows = conn.execute(
                "SELECT open_time, open, high, low, close, volume "
                "FROM candles ORDER BY open_time DESC LIMIT 672"
            ).fetchall()
            chart_bars = [
                {
                    "time_ms": int(r["open_time"]),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": float(r["volume"] or 0),
                    "forming": False,
                }
                for r in reversed(rows)
            ]

        # Acrescenta ou atualiza o candle que ainda está se formando, usando o snapshot ao vivo.
        if latest_snapshot and latest_snapshot.get("candle_open_time") is not None:
            live_bar = {
                "time_ms": int(latest_snapshot["candle_open_time"]),
                "open": float(latest_snapshot.get("open") or price or 0),
                "high": float(latest_snapshot.get("high") or price or 0),
                "low": float(latest_snapshot.get("low") or price or 0),
                "close": float(price or 0),
                "volume": float(latest_snapshot.get("volume") or 0),
                "forming": True,
            }
            if chart_bars and chart_bars[-1]["time_ms"] == live_bar["time_ms"]:
                chart_bars[-1].update(live_bar)
            else:
                chart_bars.append(live_bar)

        chart_start_ms = chart_bars[0]["time_ms"] if chart_bars else 0
        chart_markers = []
        if table_exists(conn, "trades"):
            rows = conn.execute(
                "SELECT agent_id, entry_time_ms, exit_time_ms, entry_price, exit_price, "
                "exit_reason FROM trades "
                "WHERE entry_time_ms >= ? OR exit_time_ms >= ? "
                "ORDER BY exit_time_ms DESC LIMIT 4000",
                (chart_start_ms, chart_start_ms),
            ).fetchall()
            for trade in rows:
                if trade["entry_time_ms"] is not None and trade["entry_price"] is not None:
                    chart_markers.append({
                        "agent_id": trade["agent_id"],
                        "kind": "entry",
                        "time_ms": int(trade["entry_time_ms"]),
                        "price": float(trade["entry_price"]),
                        "open": False,
                    })
                if trade["exit_time_ms"] is not None and trade["exit_price"] is not None:
                    chart_markers.append({
                        "agent_id": trade["agent_id"],
                        "kind": "exit",
                        "time_ms": int(trade["exit_time_ms"]),
                        "price": float(trade["exit_price"]),
                        "open": False,
                        "reason": trade["exit_reason"],
                    })

        # Posições ainda abertas não aparecem na tabela de trades encerrados.
        for agent in agents:
            if agent["btc"] > 0 and agent["entry_time_ms"] and agent["entry_price"]:
                chart_markers.append({
                    "agent_id": agent["id"],
                    "kind": "entry",
                    "time_ms": int(agent["entry_time_ms"]),
                    "price": float(agent["entry_price"]),
                    "open": True,
                })

        chart_markers.sort(key=lambda item: item["time_ms"])

        if age_seconds is None:
            freshness = "SEM DADOS"
        elif age_seconds <= 35:
            freshness = "DADOS RECENTES"
        elif age_seconds <= 90:
            freshness = "ATUALIZAÇÃO ATRASADA"
        else:
            freshness = "SEM ATUALIZAÇÃO RECENTE"

        return {
            "ok": True,
            "pair": config.get("par", "BTCUSDT"),
            "interval": interval,
            "price": price,
            "snapshot_time": format_time_ms(last_snapshot_ms),
            "snapshot_age_seconds": age_seconds,
            "freshness": freshness,
            "websocket_open": latest_snapshot.get("websocket_open") if latest_snapshot else None,
            "generation": generation,
            "generation_candles": generation_candles,
            "generation_target": candles_per_generation,
            "generation_percent": min(100.0, generation_candles * 100.0 / candles_per_generation),
            "population": population,
            "active_agents": sum(1 for a in agents if a["status"] == "ATIVA"),
            "starting_per_agent": starting_per_agent,
            "starting_total": starting_total,
            "equity_total": current_equity,
            "equity_change": current_equity - starting_total,
            "equity_change_percent": (
                (current_equity - starting_total) * 100.0 / starting_total
                if starting_total else None
            ),
            "cash_total": total_cash,
            "open_positions": len(open_agents),
            "open_btc": open_btc,
            "open_notional": open_notional,
            "trades": trade_summary,
            "prediction_correct": correct,
            "prediction_error": incorrect,
            "prediction_count": prediction_count,
            "prediction_accuracy": prediction_accuracy,
            "last_signal": last_signal,
            "last_signal_time": format_iso_time(last_signal_time),
            "best_agent": best_agent,
            "champion_defined": best_agent is not None,
            "action_counts": action_counts,
            "training_accuracy": (
                float(metrics["acuracia_holdout"]) * 100
                if metrics.get("acuracia_holdout") is not None else None
            ),
            "training_holdout_samples": metrics.get("amostras_holdout"),
            "candle_count": candle_count,
            "decision_count": decision_count,
            "snapshot_count": snapshot_count,
            "top_agents": top_agents,
            "recent_trades": recent_trades,
            "agent_ids": sorted(a["id"] for a in agents),
            "chart_interval_ms": candle_minutes * 60_000,
            "chart_bars": chart_bars,
            "chart_markers": chart_markers,
        }
    finally:
        conn.close()


PAGE = r"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monitor da IA — BTCUSDT</title>
<style>
:root{color-scheme:dark;--bg:#0b1220;--panel:#121c2d;--panel2:#17243a;--line:#263750;--text:#edf3fb;--muted:#9cafc5;--blue:#4da3ff;--green:#2ed28b;--red:#ff6577;--yellow:#ffd166}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 Segoe UI,Arial,sans-serif}
main{max-width:1250px;margin:auto;padding:24px}
header{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:18px}
h1{font-size:clamp(22px,3vw,32px);margin:0 0 4px}.sub{color:var(--muted)}
.badge{padding:8px 13px;border:1px solid var(--line);border-radius:999px;font-weight:700;white-space:nowrap}
.badge.good{color:var(--green);border-color:#22684e}.badge.warn{color:var(--yellow);border-color:#806b31}.badge.bad{color:var(--red);border-color:#783643}
.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px;min-width:0}
.label{color:var(--muted);font-size:13px}.value{font-size:23px;font-weight:750;margin-top:5px;overflow-wrap:anywhere}
.note{color:var(--muted);font-size:12px;margin-top:6px}
.section{margin-top:18px}.section h2{font-size:18px;margin:0 0 10px}
.two{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.signal{font-size:19px;font-weight:700;margin-top:6px}.green{color:var(--green)}.red{color:var(--red)}.yellow{color:var(--yellow)}.blue{color:var(--blue)}
.progress{height:9px;background:#263750;border-radius:9px;overflow:hidden;margin-top:10px}.bar{height:100%;background:var(--blue);width:0}
.actions{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}.chip{background:var(--panel2);border:1px solid var(--line);border-radius:8px;padding:7px 10px;color:var(--muted)}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:9px 8px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--muted);font-weight:600}td.num,th.num{text-align:right}.table-wrap{overflow:auto}
footer{color:var(--muted);font-size:12px;margin:18px 2px 4px}.error{color:var(--red);margin-top:10px}
.chart-controls{display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin:12px 0}
.chart-range{border:1px solid var(--line);border-radius:7px;background:var(--panel2);color:var(--text);padding:8px 12px;cursor:pointer}
.chart-range.active{border-color:var(--blue);background:#193456;color:#fff}
.chart-controls select{background:var(--panel2);color:var(--text);border:1px solid var(--line);border-radius:7px;padding:8px 12px}
.chart-legend{color:var(--muted);font-size:12px;display:flex;gap:16px;flex-wrap:wrap;margin:8px 0}
.chart-wrap{position:relative;width:100%;height:440px;background:#0e1725;border:1px solid var(--line);border-radius:8px;overflow:hidden}
#btcChart{width:100%;height:100%;display:block}
.chart-tooltip{display:none;position:absolute;z-index:2;pointer-events:none;background:#050a12;border:1px solid var(--line);border-radius:7px;padding:8px 10px;color:var(--text);font-size:12px;box-shadow:0 5px 20px #0009;white-space:nowrap}
.chart-empty{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:var(--muted);pointer-events:none}
@media(max-width:850px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.two{grid-template-columns:1fr}}
@media(max-width:480px){main{padding:14px}.grid{grid-template-columns:1fr 1fr}.card{padding:12px}.value{font-size:18px}header{align-items:flex-start;flex-direction:column}}
</style>
</head>
<body><main>
<header>
  <div><h1>Monitor da IA — BTCUSDT</h1><div class="sub">Painel somente leitura • atualização automática a cada 3 segundos</div></div>
  <div id="status" class="badge">Conectando ao banco…</div>
</header>

<div class="grid">
  <section class="card"><div class="label">Preço mais recente</div><div id="price" class="value">—</div><div id="snapshotTime" class="note">—</div></section>
  <section class="card"><div class="label">Geração atual</div><div id="generation" class="value">—</div><div id="generationNote" class="note">—</div><div class="progress"><div id="generationBar" class="bar"></div></div></section>
  <section class="card"><div class="label">Patrimônio fictício da população</div><div id="equity" class="value">—</div><div id="equityChange" class="note">—</div></section>
  <section class="card"><div class="label">População ativa</div><div id="population" class="value">—</div><div id="positions" class="note">—</div></section>
</div>

<div class="section two">
  <section class="card">
    <div class="label">Sinal mais recente salvo pela IA</div>
    <div id="signal" class="signal">—</div>
    <div id="signalTime" class="note">—</div>
    <div id="champion" class="note"></div>
  </section>
  <section class="card">
    <div class="label">Previsões de direção já avaliadas</div>
    <div id="accuracy" class="value">—</div>
    <div id="accuracyNote" class="note">—</div>
  </section>
</div>

<div class="section grid">
  <section class="card"><div class="label">Trades encerrados</div><div id="tradeCount" class="value">—</div><div id="tradeWins" class="note">—</div></section>
  <section class="card"><div class="label">Resultado líquido dos trades encerrados</div><div id="tradeNet" class="value">—</div><div id="tradeFees" class="note">—</div></section>
  <section class="card"><div class="label">Caixa fictício disponível</div><div id="cash" class="value">—</div><div class="note">Soma do dinheiro não investido pelos agentes</div></section>
  <section class="card"><div class="label">Posições abertas</div><div id="openPositions" class="value">—</div><div id="openValue" class="note">—</div></section>
</div>

<div class="section two">
  <section class="card"><div class="label">Atividade atual dos agentes</div><div id="actions" class="actions"></div><div id="dbCounts" class="note"></div></section>
  <section class="card"><div class="label">Referência do treino histórico</div><div id="trainAccuracy" class="value">—</div><div id="trainSamples" class="note">—</div><div class="note">É uma validação histórica; não garante o resultado futuro.</div></section>
</div>

<div class="section card">
  <h2>Agentes com maior fitness atual</h2>
  <div class="note">Classificação momentânea do modelo; ainda não significa que sejam estratégias vencedoras comprovadas.</div>
  <div class="table-wrap"><table><thead><tr>
    <th>ID</th><th>Ação</th><th class="num">Patrimônio</th><th class="num">Retorno</th><th class="num">Acerto/erro</th><th class="num">Chance de alta</th><th class="num">Risco</th>
  </tr></thead><tbody id="agents"></tbody></table></div>
</div>

<div class="section card">
  <h2>Trades encerrados mais recentes</h2>
  <div class="table-wrap"><table><thead><tr>
    <th>Agente</th><th>Horário</th><th>Saída</th><th class="num">Preço de entrada</th><th class="num">Preço de saída</th><th class="num">Resultado líquido</th>
  </tr></thead><tbody id="recentTrades"></tbody></table></div>
</div>

<section class="section card" id="chartSection">
  <h2>Gráfico BTCUSDT e operações das IAs</h2>
  <div class="note">Candles de 15 minutos. Cada cor identifica uma IA; círculo é entrada e quadrado é saída. Passe o mouse sobre uma marca para ver o horário e o preço.</div>
  <div class="chart-controls">
    <button type="button" class="chart-range" data-hours="6">6 horas</button>
    <button type="button" class="chart-range active" data-hours="24">24 horas</button>
    <button type="button" class="chart-range" data-hours="72">3 dias</button>
    <button type="button" class="chart-range" data-hours="168">7 dias</button>
    <label for="agentFilter" class="label">Filtrar IA:</label>
    <select id="agentFilter"><option value="all">Todas as IAs</option></select>
  </div>
  <div class="chart-legend">
    <span>● Entrada</span><span>■ Saída</span><span>Cor = agente</span>
    <span id="chartMarkerCount">Carregando operações…</span>
  </div>
  <div class="chart-wrap">
    <canvas id="btcChart" aria-label="Gráfico de candles BTCUSDT com entradas e saídas das IAs"></canvas>
    <div id="chartTooltip" class="chart-tooltip"></div>
    <div id="chartEmpty" class="chart-empty">Carregando candles e operações…</div>
  </div>
  <div class="note">O candle mais recente pode ainda estar em formação. As marcações mostram preços e horários registrados no banco.</div>
</section>

<div id="error" class="error"></div>
<footer>O monitor só consulta o banco; não altera a IA, não grava dados e não envia ordens. A tela acompanha a frequência com que o próprio banco recebe atualizações.</footer>
</main>
<script>
const TOKEN = __TOKEN__;
const API = "/api/status?token=" + encodeURIComponent(TOKEN);
const num = (v,d=2) => v == null || !Number.isFinite(Number(v)) ? "—" : new Intl.NumberFormat("pt-BR",{minimumFractionDigits:d,maximumFractionDigits:d}).format(Number(v));
const money = v => v == null ? "—" : num(v,2) + " USDT";
const pct = v => v == null ? "—" : num(v,1) + "%";
const esc = s => String(s ?? "—").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const signedMoney = v => (Number(v) > 0 ? "+" : "") + money(v);
function setText(id, value){ document.getElementById(id).textContent = value ?? "—"; }
function classForAction(action){
  const a=String(action||"").toUpperCase();
  if(a.includes("COMPRAR")||a.includes("ALTA")) return "green";
  if(a.includes("SAIR")||a.includes("BAIXA")) return "red";
  return "yellow";
}
function render(d){
  const badge=document.getElementById("status");
  badge.textContent=d.freshness + (d.snapshot_age_seconds == null ? "" : " • " + Math.floor(d.snapshot_age_seconds) + "s atrás");
  badge.className="badge " + (d.freshness==="DADOS RECENTES"?"good":d.freshness==="ATUALIZAÇÃO ATRASADA"?"warn":"bad");

  setText("price", d.price == null ? "Sem preço" : money(d.price));
  setText("snapshotTime", d.snapshot_time || "Ainda não há snapshots");
  setText("generation", "Geração " + (d.generation ?? "—"));
  setText("generationNote", num(d.generation_candles,0) + " de " + num(d.generation_target,0) + " candles • " + pct(d.generation_percent));
  document.getElementById("generationBar").style.width=Math.min(100,Math.max(0,Number(d.generation_percent)||0))+"%";
  setText("equity", money(d.equity_total));
  const change=d.equity_change;
  const ec=document.getElementById("equityChange");
  ec.textContent="Variação sobre a banca inicial: " + signedMoney(change) + " (" + pct(d.equity_change_percent) + ")";
  ec.className="note " + (Number(change)>0?"green":Number(change)<0?"red":"");
  setText("population", num(d.active_agents,0) + " / " + num(d.population,0) + " IAs ativas");
  setText("positions", num(d.open_positions,0) + " agentes com posição aberta");
  setText("signal", d.last_signal || "Ainda não há sinal salvo");
  document.getElementById("signal").className="signal " + classForAction(d.last_signal);
  setText("signalTime", d.last_signal_time ? "Horário: " + d.last_signal_time : "Horário do sinal não disponível");
  setText("champion", d.champion_defined ? "Agente campeão salvo: #" + d.best_agent : "Estratégia campeã: ainda não definida");
  setText("accuracy", d.prediction_accuracy == null ? "Ainda sem avaliações" : pct(d.prediction_accuracy));
  setText("accuracyNote", num(d.prediction_correct,0) + " acertos de " + num(d.prediction_count,0) + " previsões avaliadas; " + num(d.prediction_error,0) + " erros.");
  setText("tradeCount", num(d.trades.count,0));
  setText("tradeWins", num(d.trades.wins,0) + " com lucro • " + num(d.trades.losses,0) + " com prejuízo" + (d.trades.win_rate==null?"":" • " + pct(d.trades.win_rate) + " vencedores"));
  const netEl=document.getElementById("tradeNet");
  netEl.textContent=signedMoney(d.trades.net);
  netEl.className="value " + (Number(d.trades.net)>0?"green":Number(d.trades.net)<0?"red":"");
  setText("tradeFees", "Taxas pagas em trades encerrados: " + money(d.trades.fees));
  setText("cash", money(d.cash_total));
  setText("openPositions", num(d.open_positions,0) + " posições");
  setText("openValue", num(d.open_btc,6) + " BTC • valor marcado: " + money(d.open_notional));
  const actions=document.getElementById("actions");
  actions.innerHTML="";
  Object.entries(d.action_counts||{}).sort((a,b)=>b[1]-a[1]).forEach(([name,count])=>{
    const chip=document.createElement("span");chip.className="chip";chip.textContent=name+": "+count;actions.appendChild(chip);
  });
  setText("dbCounts", num(d.decision_count,0)+" decisões • "+num(d.candle_count,0)+" candles • "+num(d.snapshot_count,0)+" atualizações de preço");
  setText("trainAccuracy", d.training_accuracy==null?"Sem dado":pct(d.training_accuracy));
  setText("trainSamples", d.training_holdout_samples==null?"Amostras de validação não registradas":num(d.training_holdout_samples,0)+" amostras históricas separadas para validação");

  const agents=document.getElementById("agents");
  agents.innerHTML=(d.top_agents||[]).map(a=>{
    const ret=Number(a.equity)-Number(d.starting_per_agent||100);
    return "<tr><td>#"+esc(a.id)+"</td><td class='"+classForAction(a.action)+"'>"+esc(a.action)+"</td>"+
      "<td class='num'>"+money(a.equity)+"</td><td class='num "+(ret>0?"green":ret<0?"red":"")+"'>"+signedMoney(ret)+"</td>"+
      "<td class='num'>"+num(a.prediction_correct,0)+"/"+num(a.prediction_error,0)+"</td>"+
      "<td class='num'>"+pct(a.probability_up==null?null:Number(a.probability_up)*100)+"</td>"+
      "<td class='num'>"+pct(a.risk==null?null:Number(a.risk)*100)+"</td></tr>";
  }).join("") || "<tr><td colspan='7'>Aguardando agentes no banco.</td></tr>";

  const trades=document.getElementById("recentTrades");
  trades.innerHTML=(d.recent_trades||[]).map(t=>{
    const result=Number(t.net_pnl_usdt);
    return "<tr><td>#"+esc(t.agent_id)+"</td><td>"+esc(t.exit_time_display)+"</td><td>"+esc(t.exit_reason)+"</td>"+
      "<td class='num'>"+money(t.entry_price)+"</td><td class='num'>"+money(t.exit_price)+"</td>"+
      "<td class='num "+(result>0?"green":result<0?"red":"")+"'>"+signedMoney(result)+"</td></tr>";
  }).join("") || "<tr><td colspan='6'>Nenhum trade encerrado ainda.</td></tr>";
  document.getElementById("error").textContent="";
  renderChart(d);
}

let chartPayload=null;
let chartHours=24;
let chartAgent="all";
let chartHitTargets=[];

function agentColor(id){
  const hue=(Number(id)*137.508+17)%360;
  return "hsl("+hue+",88%,64%)";
}
function chartPrice(value){
  return new Intl.NumberFormat("pt-BR",{minimumFractionDigits:0,maximumFractionDigits:2}).format(value);
}
function chartTime(value){
  return new Date(value).toLocaleString("pt-BR",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"});
}
function populateAgentFilter(ids){
  const select=document.getElementById("agentFilter");
  const selected=select.value||chartAgent;
  const existing=Array.from(select.options).map(o=>o.value);
  const wanted=["all"].concat((ids||[]).map(String));
  if(existing.length!==wanted.length || existing.some((v,i)=>v!==wanted[i])){
    select.innerHTML="<option value='all'>Todas as IAs</option>";
    (ids||[]).forEach(id=>{
      const option=document.createElement("option");
      option.value=String(id); option.textContent="IA #"+id;
      select.appendChild(option);
    });
  }
  select.value=wanted.includes(selected)?selected:"all";
  chartAgent=select.value;
}
function renderChart(data){
  chartPayload=data;
  populateAgentFilter(data.agent_ids||[]);
  drawChart();
}
function drawChart(){
  const canvas=document.getElementById("btcChart");
  if(!canvas || !chartPayload) return;
  const ctx=canvas.getContext("2d");
  const box=canvas.getBoundingClientRect();
  const width=Math.max(300,box.width);
  const height=Math.max(250,box.height);
  const ratio=window.devicePixelRatio||1;
  canvas.width=Math.round(width*ratio);
  canvas.height=Math.round(height*ratio);
  ctx.setTransform(ratio,0,0,ratio,0,0);
  ctx.clearRect(0,0,width,height);
  chartHitTargets=[];

  const allBars=chartPayload.chart_bars||[];
  const interval=Number(chartPayload.chart_interval_ms)||900000;
  if(!allBars.length){
    document.getElementById("chartEmpty").style.display="flex";
    document.getElementById("chartEmpty").textContent="Ainda não há candles no banco.";
    document.getElementById("chartMarkerCount").textContent="Sem operações para desenhar";
    return;
  }

  const endTime=allBars[allBars.length-1].time_ms;
  const startTime=endTime-chartHours*3600000;
  const bars=allBars.filter(b=>Number(b.time_ms)>=startTime);
  const selected=chartAgent;
  const markers=(chartPayload.chart_markers||[]).filter(m=>
    Number(m.time_ms)>=startTime &&
    Number(m.time_ms)<=endTime+interval &&
    (selected==="all" || String(m.agent_id)===selected)
  );
  document.getElementById("chartEmpty").style.display="none";
  document.getElementById("chartMarkerCount").textContent=
    markers.length+" marcações neste período"+(markers.length>100?" • use o filtro de IA para reduzir sobreposição":"");

  const left=72,right=18,top=22,bottom=42;
  const plotW=width-left-right,plotH=height-top-bottom;
  const slot=plotW/Math.max(1,bars.length);
  const prices=[];
  bars.forEach(b=>prices.push(Number(b.low),Number(b.high)));
  markers.forEach(m=>prices.push(Number(m.price)));
  let min=Math.min(...prices),max=Math.max(...prices);
  if(!Number.isFinite(min)||!Number.isFinite(max)){return;}
  if(max===min){max+=1;min-=1;}
  const pad=(max-min)*0.07;
  min-=pad;max+=pad;
  const yFor=p=>top+(max-p)/(max-min)*plotH;
  const xForIndex=i=>left+(i+0.5)*slot;

  ctx.font="12px Segoe UI,Arial";
  ctx.textBaseline="middle";
  ctx.strokeStyle="#263750";
  ctx.fillStyle="#9cafc5";
  for(let i=0;i<=5;i++){
    const y=top+(plotH*i/5);
    const value=max-(max-min)*i/5;
    ctx.beginPath();ctx.moveTo(left,y);ctx.lineTo(width-right,y);ctx.stroke();
    ctx.textAlign="right";ctx.fillText(chartPrice(value),left-8,y);
  }

  for(let i=0;i<bars.length;i++){
    const b=bars[i];
    const x=xForIndex(i);
    const open=Number(b.open),close=Number(b.close),high=Number(b.high),low=Number(b.low);
    const up=close>=open;
    const color=up?"#23c987":"#f0445b";
    ctx.strokeStyle=color;ctx.fillStyle=color;ctx.lineWidth=1.2;
    ctx.beginPath();ctx.moveTo(x,yFor(high));ctx.lineTo(x,yFor(low));ctx.stroke();
    const bodyTop=Math.min(yFor(open),yFor(close));
    const bodyHeight=Math.max(1.5,Math.abs(yFor(open)-yFor(close)));
    const bodyWidth=Math.max(2,Math.min(14,slot*0.66));
    if(b.forming) ctx.globalAlpha=0.78;
    ctx.fillRect(x-bodyWidth/2,bodyTop,bodyWidth,bodyHeight);
    ctx.globalAlpha=1;
  }

  const last=bars[bars.length-1];
  if(last){
    const price=Number(last.close),y=yFor(price);
    ctx.save();ctx.setLineDash([5,4]);ctx.strokeStyle="#4da3ff";ctx.lineWidth=1;
    ctx.beginPath();ctx.moveTo(left,y);ctx.lineTo(width-right,y);ctx.stroke();ctx.restore();
    ctx.fillStyle="#4da3ff";ctx.textAlign="left";ctx.textBaseline="bottom";
    ctx.fillText("Preço atual "+chartPrice(price),left+5,Math.max(top+12,y-3));
  }

  ctx.fillStyle="#9cafc5";ctx.textAlign="center";ctx.textBaseline="top";
  const tickCount=Math.min(6,bars.length);
  for(let i=0;i<tickCount;i++){
    const index=tickCount===1?0:Math.round(i*(bars.length-1)/(tickCount-1));
    ctx.fillText(chartTime(bars[index].time_ms),xForIndex(index),height-bottom+10);
  }

  const compact=markers.length>45;
  ctx.font="bold 10px Segoe UI,Arial";
  markers.forEach((m,index)=>{
    const delta=(Number(m.time_ms)-Number(bars[0].time_ms))/interval;
    const x=left+(delta+0.5)*slot;
    if(x<left-4||x>width-right+4) return;
    const y=yFor(Number(m.price));
    const color=agentColor(m.agent_id);
    const entry=m.kind==="entry";
    ctx.lineWidth=1.5;ctx.strokeStyle="#07101d";ctx.fillStyle=color;
    if(entry){
      ctx.beginPath();ctx.arc(x,y,5,0,Math.PI*2);ctx.fill();ctx.stroke();
    }else{
      ctx.fillRect(x-4.5,y-4.5,9,9);ctx.strokeRect(x-4.5,y-4.5,9,9);
    }

    let label=compact?(entry?"E#":"S#")+m.agent_id:(entry?"Entrada IA #":"Saída IA #")+m.agent_id;
    const textWidth=ctx.measureText(label).width;
    let labelX=x+7;
    if(labelX+textWidth+9>width-right) labelX=x-textWidth-12;
    const lane=((Number(m.agent_id)*3)+(entry?0:1))%3;
    let labelY=y+(entry?-17:7)+(lane-1)*7;
    labelY=Math.max(top+2,Math.min(height-bottom-14,labelY));
    ctx.fillStyle="rgba(7,16,29,0.9)";
    ctx.fillRect(labelX-3,labelY-2,textWidth+7,14);
    ctx.fillStyle=color;ctx.textAlign="left";ctx.textBaseline="top";
    ctx.fillText(label,labelX,labelY);

    chartHitTargets.push({
      x:x,y:y,agent:m.agent_id,kind:m.kind,price:Number(m.price),
      time:Number(m.time_ms),open:Boolean(m.open),reason:m.reason||""
    });
  });
}
function setChartRange(hours,button){
  chartHours=Number(hours)||24;
  document.querySelectorAll(".chart-range").forEach(b=>b.classList.toggle("active",b===button));
  drawChart();
}
document.querySelectorAll(".chart-range").forEach(button=>{
  button.addEventListener("click",()=>setChartRange(button.dataset.hours,button));
});
document.getElementById("agentFilter").addEventListener("change",event=>{
  chartAgent=event.target.value;drawChart();
});
const chartCanvas=document.getElementById("btcChart");
const chartTooltip=document.getElementById("chartTooltip");
chartCanvas.addEventListener("mousemove",event=>{
  const rect=chartCanvas.getBoundingClientRect();
  const x=event.clientX-rect.left,y=event.clientY-rect.top;
  let nearest=null,best=13*13;
  chartHitTargets.forEach(hit=>{
    const distance=(hit.x-x)*(hit.x-x)+(hit.y-y)*(hit.y-y);
    if(distance<best){best=distance;nearest=hit;}
  });
  if(!nearest){chartTooltip.style.display="none";return;}
  const type=nearest.kind==="entry"?"Entrada":"Saída";
  const state=nearest.open?" • posição ainda aberta":(nearest.reason?" • "+nearest.reason:"");
  chartTooltip.textContent=type+" IA #"+nearest.agent+" • "+chartPrice(nearest.price)+" USDT • "+
    chartTime(nearest.time)+state;
  chartTooltip.style.display="block";
  chartTooltip.style.left=Math.min(rect.width-285,Math.max(4,x+12))+"px";
  chartTooltip.style.top=Math.max(4,y-36)+"px";
});
chartCanvas.addEventListener("mouseleave",()=>{chartTooltip.style.display="none";});
window.addEventListener("resize",drawChart);

async function refresh(){
  try{
    const response=await fetch(API,{cache:"no-store"});
    if(!response.ok) throw new Error("O monitor respondeu com erro HTTP "+response.status);
    const data=await response.json();
    if(!data.ok) throw new Error(data.error||"Não foi possível ler o banco");
    render(data);
  }catch(e){
    const badge=document.getElementById("status");badge.textContent="SEM CONEXÃO";badge.className="badge bad";
    document.getElementById("error").textContent="Não consegui atualizar o painel: "+e.message;
  }
}
refresh();setInterval(refresh,3000);
</script></body></html>"""


class MonitorServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, handler, db_path, token):
        super().__init__(address, handler)
        self.db_path = db_path
        self.monitor_token = token


class Handler(BaseHTTPRequestHandler):
    server_version = "MonitorIA/1.0"
    sys_version = ""

    def log_message(self, format_string, *args):
        # Evita imprimir URLs que contêm o token de acesso.
        return

    def client_is_local(self):
        raw_ip = self.client_address[0].split("%", 1)[0]
        try:
            address = ipaddress.ip_address(raw_ip)
            return address.is_private or address.is_loopback
        except ValueError:
            return False

    def authorized(self):
        if not self.client_is_local():
            return False
        query = parse_qs(urlsplit(self.path).query)
        supplied = query.get("token", [""])[0]
        return secrets.compare_digest(supplied, self.server.monitor_token)

    def send_bytes(self, body, content_type, status=200):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if not self.authorized():
            self.send_bytes(b"Acesso negado. Abra o link com o token mostrado no console.", "text/plain; charset=utf-8", 403)
            return

        path = urlsplit(self.path).path
        if path == "/":
            page = PAGE.replace("__TOKEN__", json.dumps(self.server.monitor_token))
            self.send_bytes(page.encode("utf-8"), "text/html; charset=utf-8")
            return

        if path == "/api/status":
            try:
                payload = get_status(self.server.db_path)
                body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
                self.send_bytes(body, "application/json; charset=utf-8")
            except Exception as exc:
                payload = {"ok": False, "error": f"Erro ao consultar o banco: {type(exc).__name__}: {exc}"}
                self.send_bytes(json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8", 500)
            return

        self.send_bytes(b"Pagina nao encontrada.", "text/plain; charset=utf-8", 404)


def local_ipv4_addresses():
    found = set()
    try:
        for item in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = item[4][0]
            address = ipaddress.ip_address(ip)
            if not address.is_loopback and address.is_private:
                found.add(ip)
    except OSError:
        pass

    # Descobre o endereço de saída local sem enviar dados para a rede.
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect(("192.0.2.1", 80))
        ip = probe.getsockname()[0]
        probe.close()
        address = ipaddress.ip_address(ip)
        if not address.is_loopback and address.is_private:
            found.add(ip)
    except OSError:
        pass
    return sorted(found)


def main():
    parser = argparse.ArgumentParser(
        description="Painel web local, somente leitura, para a IA Evolutiva BTC."
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="caminho do banco SQLite")
    parser.add_argument("--host", default="0.0.0.0", help="interface de rede (padrão: todas)")
    parser.add_argument("--port", type=int, default=8765, help="porta HTTP (padrão: 8765)")
    args = parser.parse_args()

    db_path = args.db.expanduser().resolve()
    if not db_path.exists():
        raise SystemExit(
            "Banco não encontrado: " + str(db_path) +
            "\nDeixe este script na pasta do banco ou informe --db com o caminho correto."
        )

    token = secrets.token_urlsafe(24)
    try:
        server = MonitorServer((args.host, args.port), Handler, db_path, token)
    except OSError as exc:
        raise SystemExit(
            f"Não consegui abrir a porta {args.port}: {exc}\n"
            "Feche outro monitor que esteja usando essa porta ou escolha outra com --port."
        )

    addresses = local_ipv4_addresses()
    print("=" * 72)
    print("MONITOR DA IA EVOLUTIVA — BTCUSDT")
    print("Banco: " + db_path.name)
    print("Modo: somente leitura; nenhum arquivo do banco será alterado.")
    print(f"Atualização do painel: a cada {POLL_SECONDS} segundos.")
    print(f"Servidor ativo na porta {args.port}.")
    if addresses:
        print("\nNo computador principal, abra um destes endereços:")
        for ip in addresses:
            print(f"  http://{ip}:{args.port}/?token={token}")
    else:
        print("\nNão consegui detectar o IP local automaticamente.")
        print(f"Use o IP deste computador com a porta {args.port} e o token abaixo.")
        print("Token: " + token)
    print("\nOs dois computadores precisam estar na mesma rede local.")
    print("Se o Firewall do Windows perguntar, permita acesso somente em rede privada.")
    print("Pressione Ctrl+C para encerrar o monitor.")
    print("=" * 72)

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nMonitor encerrado.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
