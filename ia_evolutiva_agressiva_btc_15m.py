"""IA evolutiva de paper trading para BTCUSDT em candles de 15 minutos.

Cada agente tem uma banca simulada de 100 USDT e um modelo logistico simples.
Os pesos aprendem com os candles que ja fecharam; a populacao evolui por
selecao, cruzamento e mutacao. O programa nunca envia ordens a uma corretora.
"""

from __future__ import annotations

import json
import math
import queue
import random
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    import websocket
except ImportError:
    websocket = None


SYMBOL = "BTCUSDT"
INTERVAL = "15m"
INTERVAL_MS = 15 * 60 * 1000
DAY_MS = 24 * 60 * 60 * 1000
API_ROOT = "https://data-api.binance.vision/api/v3"
WS_URL = f"wss://data-stream.binance.vision/ws/{SYMBOL.lower()}@kline_{INTERVAL}"
DATA_DIR = Path(__file__).resolve().parent / "ia_evolutiva_agressiva_btc_15m_dados"
DB_PATH = DATA_DIR / "memoria_ia_evolutiva_agressiva.sqlite3"

INITIAL_BANK = 100.0
DEFAULT_POPULATION = 20
MIN_POPULATION = 2
MAX_POPULATION = 100
HISTORY_DAYS = 365
GENERATION_DAYS = 7
GENERATION_CANDLES = GENERATION_DAYS * 24 * 60 // 15
FEE_BPS_PER_SIDE = 10.0
SLIPPAGE_BPS_PER_SIDE = 5.0
MIN_ORDER_USDT = 5.0
SNAPSHOT_SECONDS = 10.0
SAVE_SECONDS = 5.0
ONLINE_LEARNING_RATE_DEFAULT = 0.015
FEATURE_NAMES = (
    "retorno_1", "retorno_2", "retorno_4", "retorno_8",
    "corpo_candle", "amplitude_candle", "volume_relativo",
    "distancia_ema12", "distancia_ema48", "rsi14",
)
FEATURE_COUNT = len(FEATURE_NAMES)


def utc_text(timestamp_ms: int | None = None) -> str:
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def sigmoid(value: float) -> float:
    value = clamp(value, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-value))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def api_json(endpoint: str, params: dict[str, Any] | None = None) -> Any:
    query = f"?{urlencode(params)}" if params else ""
    request = Request(
        f"{API_ROOT}/{endpoint}{query}",
        headers={"User-Agent": "Codex-IaEvolutiva-Paper/1.0"},
    )
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            with urlopen(request, timeout=25) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
            if isinstance(error, HTTPError) and error.code not in (418, 429, 500, 502, 503, 504):
                break
            time.sleep(min(2 ** attempt, 8))
    raise RuntimeError(f"Falha na API publica da Binance ({endpoint}): {last_error}")


def normalize_kline(row: list[Any]) -> dict[str, Any]:
    return {
        "open_time": int(row[0]),
        "open": safe_float(row[1]),
        "high": safe_float(row[2]),
        "low": safe_float(row[3]),
        "close": safe_float(row[4]),
        "volume": safe_float(row[5]),
        "close_time": int(row[6]),
        "quote_volume": safe_float(row[7]),
        "trades": int(row[8]),
    }


def normalize_ws_kline(event: dict[str, Any]) -> dict[str, Any] | None:
    item = event.get("k")
    if not isinstance(item, dict):
        return None
    return {
        "open_time": int(item["t"]),
        "open": safe_float(item["o"]),
        "high": safe_float(item["h"]),
        "low": safe_float(item["l"]),
        "close": safe_float(item["c"]),
        "volume": safe_float(item["v"]),
        "close_time": int(item["T"]),
        "quote_volume": safe_float(item.get("q")),
        "trades": int(item.get("n", 0)),
        "closed": bool(item.get("x")),
    }


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS candles (
                open_time INTEGER PRIMARY KEY,
                open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
                close REAL NOT NULL, volume REAL NOT NULL,
                close_time INTEGER NOT NULL, quote_volume REAL NOT NULL,
                trades INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS generations (
                generation INTEGER PRIMARY KEY,
                started_utc TEXT NOT NULL,
                ended_utc TEXT,
                population_size INTEGER NOT NULL,
                reason TEXT,
                summary_json TEXT
            );
            CREATE TABLE IF NOT EXISTS agents (
                agent_id INTEGER PRIMARY KEY,
                generation INTEGER NOT NULL,
                parent_id INTEGER,
                genome_json TEXT NOT NULL,
                state_json TEXT NOT NULL,
                fitness REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                started_utc TEXT NOT NULL,
                ended_utc TEXT
            );
            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candle_open_time INTEGER NOT NULL,
                decision_utc TEXT NOT NULL,
                generation INTEGER NOT NULL,
                agent_id INTEGER NOT NULL,
                probability_up REAL NOT NULL,
                action TEXT NOT NULL,
                features_json TEXT NOT NULL,
                genome_json TEXT NOT NULL,
                equity_usdt REAL NOT NULL,
                points_correct INTEGER NOT NULL,
                points_error INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_decisions_agent_time
                ON decisions(agent_id, candle_open_time);
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id INTEGER NOT NULL,
                generation INTEGER NOT NULL,
                parent_id INTEGER,
                entry_time_ms INTEGER NOT NULL,
                exit_time_ms INTEGER NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL NOT NULL,
                quantity_btc REAL NOT NULL,
                gross_pnl_usdt REAL NOT NULL,
                entry_fee_usdt REAL NOT NULL,
                exit_fee_usdt REAL NOT NULL,
                net_pnl_usdt REAL NOT NULL,
                exit_reason TEXT NOT NULL,
                probability_at_entry REAL,
                stop_price REAL,
                target_price REAL,
                genome_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trades_agent_time
                ON trades(agent_id, exit_time_ms);
            CREATE TABLE IF NOT EXISTS snapshots (
                timestamp_ms INTEGER PRIMARY KEY,
                candle_open_time INTEGER NOT NULL,
                open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
                price REAL NOT NULL, volume REAL NOT NULL,
                websocket_open INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_utc TEXT NOT NULL,
                generation INTEGER,
                event_type TEXT NOT NULL,
                details_json TEXT NOT NULL
            );
            """
        )
        self.db.commit()

    def get_meta(self, key: str, default: Any = None) -> Any:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return default

    def set_meta(self, key: str, value: Any) -> None:
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json_text(value)),
        )

    def insert_candle(self, candle: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO candles VALUES(?,?,?,?,?,?,?,?,?)",
            (
                candle["open_time"], candle["open"], candle["high"], candle["low"],
                candle["close"], candle["volume"], candle["close_time"],
                candle["quote_volume"], candle["trades"],
            ),
        )

    def candle_rows(self) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT * FROM candles ORDER BY open_time"
        ).fetchall()
        return [dict(row) for row in rows]

    def candles_after(self, open_time: int) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT * FROM candles WHERE open_time>? ORDER BY open_time", (open_time,)
        ).fetchall()
        return [dict(row) for row in rows]

    def latest_candle_open(self) -> int | None:
        row = self.db.execute("SELECT MAX(open_time) AS value FROM candles").fetchone()
        return int(row["value"]) if row and row["value"] is not None else None

    def load_agents(self, generation: int) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT state_json FROM agents WHERE generation=? ORDER BY agent_id",
            (generation,),
        ).fetchall()
        return [json.loads(row["state_json"]) for row in rows]

    def save_agent(self, agent: dict[str, Any], status: str | None = None) -> None:
        current_status = status or agent["status"]
        agent["status"] = current_status
        fitness = agent_fitness(agent, agent.get("last_price", agent.get("entry_price") or 0.0))
        self.db.execute(
            """INSERT INTO agents(
                   agent_id,generation,parent_id,genome_json,state_json,fitness,
                   status,started_utc,ended_utc
               ) VALUES(?,?,?,?,?,?,?,?,?)
               ON CONFLICT(agent_id) DO UPDATE SET
                   generation=excluded.generation,parent_id=excluded.parent_id,
                   genome_json=excluded.genome_json,state_json=excluded.state_json,
                   fitness=excluded.fitness,status=excluded.status,
                   ended_utc=CASE WHEN excluded.status IN ('ARQUIVADA','FALIDA')
                                  THEN COALESCE(agents.ended_utc,excluded.ended_utc)
                                  ELSE agents.ended_utc END""",
            (
                agent["id"], agent["generation"], agent.get("parent_id"),
                json_text(genome_of(agent)), json_text(agent), fitness, current_status,
                agent.get("started_utc", utc_text()),
                utc_text() if current_status in ("ARQUIVADA", "FALIDA") else None,
            ),
        )

    def save_decision(self, candle: dict[str, Any], agent: dict[str, Any], features: list[float], action: str) -> None:
        self.db.execute(
            """INSERT INTO decisions(
                candle_open_time,decision_utc,generation,agent_id,probability_up,
                action,features_json,genome_json,equity_usdt,points_correct,points_error
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                candle["open_time"], utc_text(candle["close_time"]), agent["generation"],
                agent["id"], agent["last_probability"], action,
                json_text(dict(zip(FEATURE_NAMES, features))), json_text(genome_of(agent)),
                agent["equity"], agent["points_correct"], agent["points_error"],
            ),
        )

    def save_trade(self, trade: dict[str, Any]) -> None:
        self.db.execute(
            """INSERT INTO trades(
                agent_id,generation,parent_id,entry_time_ms,exit_time_ms,entry_price,
                exit_price,quantity_btc,gross_pnl_usdt,entry_fee_usdt,exit_fee_usdt,
                net_pnl_usdt,exit_reason,probability_at_entry,stop_price,target_price,genome_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                trade["agent_id"], trade["generation"], trade.get("parent_id"),
                trade["entry_time_ms"], trade["exit_time_ms"], trade["entry_price"],
                trade["exit_price"], trade["quantity_btc"], trade["gross_pnl_usdt"],
                trade["entry_fee_usdt"], trade["exit_fee_usdt"], trade["net_pnl_usdt"],
                trade["exit_reason"], trade.get("probability_at_entry"),
                trade.get("stop_price"), trade.get("target_price"),
                json_text(trade["genome"]),
            ),
        )

    def save_snapshot(self, candle: dict[str, Any], timestamp_ms: int, websocket_open: bool) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO snapshots VALUES(?,?,?,?,?,?,?,?)",
            (
                timestamp_ms, candle["open_time"], candle["open"], candle["high"],
                candle["low"], candle["close"], candle["volume"],
                1 if websocket_open else 0,
            ),
        )

    def event(self, event_type: str, details: dict[str, Any], generation: int | None = None) -> None:
        self.db.execute(
            "INSERT INTO events(timestamp_utc,generation,event_type,details_json) VALUES(?,?,?,?)",
            (utc_text(), generation, event_type, json_text(details)),
        )

    def close(self) -> None:
        self.db.commit()
        self.db.close()


def genome_of(agent: dict[str, Any]) -> dict[str, Any]:
    return {
        "weights": agent["weights"],
        "bias": agent["bias"],
        "entry_threshold": agent["entry_threshold"],
        "risk_per_trade": agent["risk_per_trade"],
        "stop_pct": agent["stop_pct"],
        "reward_ratio": agent["reward_ratio"],
        "learning_rate": agent["learning_rate"],
    }


def agent_fitness(agent: dict[str, Any], price: float) -> float:
    equity = safe_float(agent.get("cash")) + safe_float(agent.get("btc")) * max(price, 0.0)
    initial = max(safe_float(agent.get("initial_bank"), INITIAL_BANK), 0.01)
    net_return = equity / initial - 1.0
    drawdown = safe_float(agent.get("max_drawdown"))
    predictions = agent.get("prediction_correct", 0) + agent.get("prediction_error", 0)
    directional_edge = (
        (agent.get("prediction_correct", 0) - agent.get("prediction_error", 0)) / predictions
        if predictions else 0.0
    )
    trades = int(agent.get("trade_count", 0))
    low_sample_penalty = 0.03 if trades < 3 else 0.0
    return net_return - 1.5 * drawdown + 0.03 * directional_edge - low_sample_penalty


def rsi_at(closes: list[float], index: int, period: int = 14) -> float:
    start = max(1, index - period + 1)
    gains = 0.0
    losses = 0.0
    for cursor in range(start, index + 1):
        delta = closes[cursor] - closes[cursor - 1]
        if delta > 0:
            gains += delta
        else:
            losses -= delta
    count = max(index - start + 1, 1)
    average_gain = gains / count
    average_loss = losses / count
    if average_loss == 0:
        return 100.0 if average_gain else 50.0
    relative_strength = average_gain / average_loss
    return 100.0 - 100.0 / (1.0 + relative_strength)


def feature_rows(candles: list[dict[str, Any]]) -> list[list[float] | None]:
    count = len(candles)
    if not count:
        return []
    closes = [c["close"] for c in candles]
    volumes = [c["volume"] for c in candles]
    ema12 = [closes[0]] * count
    ema48 = [closes[0]] * count
    alpha12 = 2.0 / 13.0
    alpha48 = 2.0 / 49.0
    for index in range(1, count):
        ema12[index] = alpha12 * closes[index] + (1.0 - alpha12) * ema12[index - 1]
        ema48[index] = alpha48 * closes[index] + (1.0 - alpha48) * ema48[index - 1]
    prefix_volume = [0.0]
    for value in volumes:
        prefix_volume.append(prefix_volume[-1] + value)
    output: list[list[float] | None] = [None] * count
    for index in range(49, count):
        candle = candles[index]
        close = max(candle["close"], 1e-12)
        open_price = max(candle["open"], 1e-12)
        returns = [
            (close / max(closes[index - lag], 1e-12) - 1.0) * 100.0
            for lag in (1, 2, 4, 8)
        ]
        body = (close / open_price - 1.0) * 100.0
        amplitude = (candle["high"] - candle["low"]) / open_price * 100.0
        volume_start = max(0, index - 20)
        average_volume = (prefix_volume[index] - prefix_volume[volume_start]) / max(index - volume_start, 1)
        volume_ratio = math.log((candle["volume"] + 1e-12) / (average_volume + 1e-12))
        ema12_gap = (close / max(ema12[index], 1e-12) - 1.0) * 100.0
        ema48_gap = (close / max(ema48[index], 1e-12) - 1.0) * 100.0
        rsi = (rsi_at(closes, index) - 50.0) / 50.0
        values = [
            clamp(returns[0] / 3.0, -1.0, 1.0),
            clamp(returns[1] / 5.0, -1.0, 1.0),
            clamp(returns[2] / 8.0, -1.0, 1.0),
            clamp(returns[3] / 12.0, -1.0, 1.0),
            clamp(body / 3.0, -1.0, 1.0),
            clamp(amplitude / 5.0, 0.0, 1.0),
            clamp(volume_ratio / 3.0, -1.0, 1.0),
            clamp(ema12_gap / 3.0, -1.0, 1.0),
            clamp(ema48_gap / 5.0, -1.0, 1.0),
            clamp(rsi, -1.0, 1.0),
        ]
        output[index] = values
    return output


def predict_probability(weights: list[float], bias: float, features: list[float]) -> float:
    return sigmoid(bias + sum(w * x for w, x in zip(weights, features)))


def fit_logistic(samples: list[tuple[list[float], int]]) -> tuple[list[float], float, dict[str, Any]]:
    weights = [0.0] * FEATURE_COUNT
    bias = 0.0
    if len(samples) < 100:
        return weights, bias, {"amostras_treino": len(samples), "acuracia_holdout": None}
    split = max(1, int(len(samples) * 0.8))
    training = samples[:split]
    holdout = samples[split:]
    learning_rate = 0.20
    regularization = 0.002
    for _ in range(60):
        gradients = [0.0] * FEATURE_COUNT
        bias_gradient = 0.0
        for features, label in training:
            prediction = predict_probability(weights, bias, features)
            difference = prediction - label
            bias_gradient += difference
            for index, value in enumerate(features):
                gradients[index] += difference * value
        scale = 1.0 / len(training)
        bias -= learning_rate * bias_gradient * scale
        for index in range(FEATURE_COUNT):
            gradient = gradients[index] * scale + regularization * weights[index]
            weights[index] -= learning_rate * gradient
    if holdout:
        correct = 0
        confidence: list[float] = []
        for features, label in holdout:
            probability = predict_probability(weights, bias, features)
            correct += int((probability >= 0.5) == bool(label))
            confidence.append(abs(probability - 0.5) * 2.0)
        accuracy = correct / len(holdout)
        mean_confidence = sum(confidence) / len(confidence)
    else:
        accuracy = None
        mean_confidence = None
    return weights, bias, {
        "amostras_treino": len(training),
        "amostras_holdout": len(holdout),
        "acuracia_holdout": accuracy,
        "confianca_media_holdout": mean_confidence,
        "periodo_holdout_inicio_ms": None,
    }


def training_samples(candles: list[dict[str, Any]]) -> tuple[list[tuple[list[float], int]], list[list[float] | None]]:
    features = feature_rows(candles)
    samples: list[tuple[list[float], int]] = []
    for index in range(len(candles) - 1):
        row = features[index]
        if row is None:
            continue
        next_candle = candles[index + 1]
        label = int(next_candle["close"] > next_candle["open"])
        samples.append((row, label))
    return samples, features


def ask_population(saved_size: int | None) -> int:
    if saved_size is None:
        prompt = f"Quantidade de IAs na população [{DEFAULT_POPULATION}]: "
        default = DEFAULT_POPULATION
    else:
        prompt = (
            f"População salva: {saved_size}. Enter mantém; digite outro número "
            "para aplicar na próxima geração: "
        )
        default = saved_size
    while True:
        try:
            raw = input(prompt).strip()
        except EOFError:
            raw = ""
        if not raw:
            return default
        try:
            value = int(raw)
        except ValueError:
            print("Digite um número inteiro.")
            continue
        if MIN_POPULATION <= value <= MAX_POPULATION:
            return value
        print(f"Escolha entre {MIN_POPULATION} e {MAX_POPULATION} IAs.")


def sync_history(store: Store, days: int = HISTORY_DAYS, quiet: bool = False) -> tuple[int, int]:
    server_time = int(api_json("time")["serverTime"])
    latest_closed_open = (server_time // INTERVAL_MS) * INTERVAL_MS - INTERVAL_MS
    latest_saved = store.latest_candle_open()
    if latest_saved is None:
        start = ((server_time - days * DAY_MS) // INTERVAL_MS) * INTERVAL_MS
    else:
        start = latest_saved + INTERVAL_MS
    downloaded = 0
    cursor = start
    while cursor <= latest_closed_open:
        rows = api_json(
            "klines",
            {
                "symbol": SYMBOL,
                "interval": INTERVAL,
                "startTime": cursor,
                "endTime": latest_closed_open + INTERVAL_MS - 1,
                "limit": 1000,
            },
        )
        if not isinstance(rows, list) or not rows:
            break
        last_open = cursor
        for row in rows:
            candle = normalize_kline(row)
            if candle["open_time"] > latest_closed_open:
                continue
            store.insert_candle(candle)
            downloaded += 1
            last_open = candle["open_time"]
        if last_open < cursor:
            break
        cursor = last_open + INTERVAL_MS
        if not quiet:
            print(f"\rCandles historicos carregados/atualizados: {downloaded:,}", end="", flush=True)
        if len(rows) < 1000:
            break
        time.sleep(0.08)
    store.db.commit()
    if not quiet:
        print()
    return server_time, downloaded


def fresh_agent(agent_id: int, generation: int, parent_id: int | None, genome: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": agent_id,
        "generation": generation,
        "parent_id": parent_id,
        "started_utc": utc_text(),
        "weights": list(genome["weights"]),
        "bias": float(genome["bias"]),
        "entry_threshold": float(genome["entry_threshold"]),
        "risk_per_trade": float(genome["risk_per_trade"]),
        "stop_pct": float(genome["stop_pct"]),
        "reward_ratio": float(genome["reward_ratio"]),
        "learning_rate": float(genome["learning_rate"]),
        "cash": INITIAL_BANK,
        "btc": 0.0,
        "entry_time_ms": None,
        "entry_price": None,
        "entry_notional": 0.0,
        "entry_fee": 0.0,
        "stop_price": None,
        "target_price": None,
        "entry_probability": None,
        "pending_action": None,
        "pending_after_open_ms": None,
        "forecast": None,
        "last_probability": 0.5,
        "last_action": "AGUARDAR",
        "last_price": 0.0,
        "equity": INITIAL_BANK,
        "peak_equity": INITIAL_BANK,
        "max_drawdown": 0.0,
        "points_correct": 0,
        "points_error": 0,
        "prediction_correct": 0,
        "prediction_error": 0,
        "trade_count": 0,
        "trade_wins": 0,
        "trade_losses": 0,
        "status": "ATIVA",
    }


def mutate_genome(parent_a: dict[str, Any], parent_b: dict[str, Any] | None, rng: random.Random) -> dict[str, Any]:
    second = parent_b or parent_a
    weights: list[float] = []
    for left, right in zip(parent_a["weights"], second["weights"]):
        base = left if rng.random() < 0.5 else right
        weights.append(clamp(base + rng.gauss(0.0, 0.12), -4.0, 4.0))
    def inherited(key: str, sigma: float, low: float, high: float) -> float:
        base = parent_a[key] if rng.random() < 0.5 else second[key]
        return clamp(float(base) + rng.gauss(0.0, sigma), low, high)
    return {
        "weights": weights,
        "bias": inherited("bias", 0.12, -3.0, 3.0),
        "entry_threshold": inherited("entry_threshold", 0.025, 0.50, 0.65),
        "risk_per_trade": inherited("risk_per_trade", 0.008, 0.01, 0.06),
        "stop_pct": inherited("stop_pct", 0.003, 0.003, 0.04),
        "reward_ratio": inherited("reward_ratio", 0.12, 1.0, 3.5),
        "learning_rate": inherited("learning_rate", 0.003, 0.003, 0.04),
    }


def new_population(
    store: Store,
    generation: int,
    population_size: int,
    parents: list[dict[str, Any]],
    seed_genome: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rng = random.Random((int(time.time() * 1000) ^ generation) & 0xFFFFFFFF)
    if not parents:
        if seed_genome is None:
            seed_genome = {
                "weights": [rng.gauss(0.0, 0.2) for _ in range(FEATURE_COUNT)],
                "bias": 0.0,
                "entry_threshold": 0.53,
                "risk_per_trade": 0.04,
                "stop_pct": 0.012,
                "reward_ratio": 1.8,
                "learning_rate": ONLINE_LEARNING_RATE_DEFAULT,
            }
        parents = [{"id": None, **seed_genome}]
    ranked = sorted(
        parents,
        key=lambda item: agent_fitness(item, item.get("last_price", 0.0))
        if "cash" in item else float(item.get("fitness", 0.0)),
        reverse=True,
    )
    parent_pool = ranked[: max(1, math.ceil(len(ranked) * 0.5))]
    best_genome = genome_of(ranked[0]) if "cash" in ranked[0] else {key: ranked[0][key] for key in (
        "weights", "bias", "entry_threshold", "risk_per_trade", "stop_pct", "reward_ratio", "learning_rate"
    )}
    elite_count = max(1, math.ceil(population_size * 0.1))
    genomes: list[tuple[dict[str, Any], int | None]] = [(best_genome, ranked[0].get("id"))]
    while len(genomes) < elite_count:
        parent = parent_pool[(len(genomes) - 1) % len(parent_pool)]
        genome = genome_of(parent) if "cash" in parent else {key: parent[key] for key in best_genome}
        genomes.append((genome, parent.get("id")))
    while len(genomes) < population_size:
        first = rng.choice(parent_pool)
        second = rng.choice(parent_pool) if len(parent_pool) > 1 else first
        genome = mutate_genome(genome_of(first) if "cash" in first else first,
                               genome_of(second) if "cash" in second else second, rng)
        genomes.append((genome, first.get("id")))
    current_max = store.db.execute("SELECT COALESCE(MAX(agent_id),0) AS n FROM agents").fetchone()["n"]
    parents_by_id = {item.get("id"): item for item in parents if item.get("id") is not None}
    created: list[dict[str, Any]] = []
    for offset, (genome, parent_id) in enumerate(genomes, start=1):
        agent = fresh_agent(int(current_max) + offset, generation, parent_id, genome)
        parent = parents_by_id.get(parent_id)
        agent["inherited_fitness"] = (
            agent_fitness(parent, parent.get("last_price", 0.0))
            if parent and "cash" in parent
            else safe_float(parent.get("fitness")) if parent else 0.0
        )
        agent["inherited_points_correct"] = int(parent.get("points_correct", 0)) if parent else 0
        agent["inherited_points_error"] = int(parent.get("points_error", 0)) if parent else 0
        agent["inherited_trade_count"] = int(parent.get("trade_count", 0)) if parent else 0
        store.save_agent(agent)
        created.append(agent)
    start_time = utc_text()
    store.db.execute(
        "INSERT OR REPLACE INTO generations(generation,started_utc,ended_utc,population_size,reason,summary_json) "
        "VALUES(?,?,NULL,?,NULL,NULL)",
        (generation, start_time, population_size),
    )
    store.set_meta("current_generation", generation)
    store.set_meta("generation_candles", 0)
    store.set_meta("generation_start_utc", start_time)
    store.set_meta("population_size", population_size)
    store.db.commit()
    store.event("NOVA_GERACAO", {
        "geracao": generation,
        "populacao": population_size,
        "banca_por_ia_usdt": INITIAL_BANK,
        "pai_melhor_id": ranked[0].get("id"),
    }, generation)
    store.db.commit()
    return created


def initialize_population(store: Store, candles: list[dict[str, Any]], population_size: int) -> list[dict[str, Any]]:
    samples, _ = training_samples(candles)
    if len(samples) >= 100:
        base_weights, base_bias, metrics = fit_logistic(samples)
        holdout_start = int(len(samples) * 0.8)
        holdout = samples[holdout_start:]
        metrics["periodo_holdout_inicio_ms"] = (
            candles[max(0, min(len(candles) - 1, int(len(candles) * 0.8)))]["open_time"]
            if candles else None
        )
    else:
        base_weights = [0.0] * FEATURE_COUNT
        base_bias = 0.0
        metrics = {"amostras_treino": 0, "amostras_holdout": 0, "acuracia_holdout": None}
        holdout = []
    base_genome = {
        "weights": base_weights,
        "bias": base_bias,
        "entry_threshold": 0.53,
        "risk_per_trade": 0.04,
        "stop_pct": 0.012,
        "reward_ratio": 1.8,
        "learning_rate": ONLINE_LEARNING_RATE_DEFAULT,
    }
    rng = random.Random(int(time.time()))
    genomes: list[tuple[dict[str, Any], int | None]] = [(base_genome, None)]
    while len(genomes) < population_size:
        genome = mutate_genome(base_genome, base_genome, rng)
        genomes.append((genome, None))
    current_max = store.db.execute("SELECT COALESCE(MAX(agent_id),0) AS n FROM agents").fetchone()["n"]
    generation = 1
    agents: list[dict[str, Any]] = []
    for offset, (genome, parent_id) in enumerate(genomes, start=1):
        agent = fresh_agent(int(current_max) + offset, generation, parent_id, genome)
        store.save_agent(agent)
        agents.append(agent)
    start_time = utc_text()
    store.db.execute(
        "INSERT OR REPLACE INTO generations(generation,started_utc,ended_utc,population_size,reason,summary_json) "
        "VALUES(?,?,NULL,?,NULL,NULL)",
        (generation, start_time, population_size),
    )
    store.set_meta("current_generation", generation)
    store.set_meta("generation_candles", 0)
    store.set_meta("generation_start_utc", start_time)
    store.set_meta("last_processed_open_time", candles[-1]["open_time"] if candles else 0)
    store.set_meta("population_size", population_size)
    store.set_meta("population_next", population_size)
    store.set_meta("training_metrics", metrics)
    store.set_meta("global_best_fitness", -999.0)
    store.set_meta("global_best_genome", base_genome)
    store.set_meta("global_best_agent_id", None)
    store.set_meta("global_best_metrics", {"fitness": -999.0, "points_correct": 0, "points_error": 0, "trade_count": 0})
    store.set_meta("feature_names", FEATURE_NAMES)
    store.event("TREINO_INICIAL", {
        "par": SYMBOL,
        "intervalo": INTERVAL,
        "amostras": len(samples),
        "metricas": metrics,
        "observacao": "Holdout cronologico apenas informativo; nao usado para ajustar o modelo.",
    }, generation)
    store.db.commit()
    return agents


def execution_buy_price(price: float) -> float:
    return price * (1.0 + SLIPPAGE_BPS_PER_SIDE / 10_000.0)


def execution_sell_price(price: float) -> float:
    return price * (1.0 - SLIPPAGE_BPS_PER_SIDE / 10_000.0)


def update_equity(agent: dict[str, Any], price: float) -> None:
    agent["last_price"] = price
    equity = agent["cash"] + agent["btc"] * price
    agent["equity"] = equity
    agent["peak_equity"] = max(agent.get("peak_equity", INITIAL_BANK), equity)
    if agent["peak_equity"] > 0:
        drawdown = max(0.0, (agent["peak_equity"] - equity) / agent["peak_equity"])
        agent["max_drawdown"] = max(agent.get("max_drawdown", 0.0), drawdown)
    if equity <= 0.01:
        agent["status"] = "FALIDA"


def open_long(agent: dict[str, Any], price: float, time_ms: int) -> bool:
    if agent["status"] != "ATIVA" or agent["btc"] > 0 or agent["cash"] < MIN_ORDER_USDT:
        return False
    fill = execution_buy_price(price)
    stop_fraction = max(agent["stop_pct"], 0.003)
    risk_budget = agent["cash"] * agent["risk_per_trade"]
    notional = min(agent["cash"] / (1.0 + FEE_BPS_PER_SIDE / 10_000.0), risk_budget / stop_fraction)
    if notional < MIN_ORDER_USDT:
        return False
    quantity = notional / fill
    entry_fee = notional * FEE_BPS_PER_SIDE / 10_000.0
    agent["cash"] -= notional + entry_fee
    agent["btc"] = quantity
    agent["entry_time_ms"] = time_ms
    agent["entry_price"] = fill
    agent["entry_notional"] = notional
    agent["entry_fee"] = entry_fee
    agent["stop_price"] = fill * (1.0 - stop_fraction)
    agent["target_price"] = fill * (1.0 + stop_fraction * agent["reward_ratio"])
    agent["entry_probability"] = agent.get("last_probability", 0.5)
    return True


def close_long(
    store: Store,
    agent: dict[str, Any],
    raw_price: float,
    time_ms: int,
    reason: str,
) -> None:
    if agent["btc"] <= 0 or agent["entry_price"] is None:
        return
    exit_price = execution_sell_price(max(raw_price, 0.0))
    quantity = agent["btc"]
    gross = quantity * (exit_price - agent["entry_price"])
    exit_fee = quantity * exit_price * FEE_BPS_PER_SIDE / 10_000.0
    net = gross - safe_float(agent["entry_fee"]) - exit_fee
    agent["cash"] += quantity * exit_price - exit_fee
    agent["trade_count"] += 1
    if net > 0:
        agent["trade_wins"] += 1
        agent["points_correct"] += 1
    elif net < 0:
        agent["trade_losses"] += 1
        agent["points_error"] += 1
    store.save_trade({
        "agent_id": agent["id"],
        "generation": agent["generation"],
        "parent_id": agent.get("parent_id"),
        "entry_time_ms": int(agent["entry_time_ms"] or time_ms),
        "exit_time_ms": int(time_ms),
        "entry_price": float(agent["entry_price"]),
        "exit_price": exit_price,
        "quantity_btc": quantity,
        "gross_pnl_usdt": gross,
        "entry_fee_usdt": float(agent["entry_fee"]),
        "exit_fee_usdt": exit_fee,
        "net_pnl_usdt": net,
        "exit_reason": reason,
        "probability_at_entry": agent.get("entry_probability"),
        "stop_price": agent.get("stop_price"),
        "target_price": agent.get("target_price"),
        "genome": genome_of(agent),
    })
    agent["btc"] = 0.0
    agent["entry_time_ms"] = None
    agent["entry_price"] = None
    agent["entry_notional"] = 0.0
    agent["entry_fee"] = 0.0
    agent["stop_price"] = None
    agent["target_price"] = None
    agent["entry_probability"] = None
    agent["last_action"] = "SAIR"
    update_equity(agent, exit_price)


def execute_pending(
    store: Store,
    agent: dict[str, Any],
    candle: dict[str, Any],
    execution_time_ms: int | None = None,
) -> None:
    pending = agent.get("pending_action")
    after_open = agent.get("pending_after_open_ms")
    if not pending or after_open is None or candle["open_time"] <= after_open:
        return
    execution_time_ms = execution_time_ms or candle["open_time"]
    if pending == "COMPRAR":
        open_long(agent, candle["open"], execution_time_ms)
    elif pending == "SAIR" and agent["btc"] > 0:
        close_long(store, agent, candle["open"], execution_time_ms, "SINAL_BAIXA")
    agent["pending_action"] = None
    agent["pending_after_open_ms"] = None


def check_intrabar(
    store: Store,
    agent: dict[str, Any],
    candle: dict[str, Any],
    execution_time_ms: int | None = None,
) -> None:
    if agent["btc"] <= 0:
        return
    stop = agent.get("stop_price")
    target = agent.get("target_price")
    if stop is None or target is None:
        return
    stop_touched = candle["low"] <= stop
    target_touched = candle["high"] >= target
    if not stop_touched and not target_touched:
        return
    execution_time_ms = execution_time_ms or candle["close_time"]
    if candle["open"] <= stop:
        close_long(store, agent, candle["open"], execution_time_ms, "STOP_GAP")
    elif stop_touched and target_touched:
        # Com OHLC de 15m nao sabemos qual nivel foi atingido primeiro.
        # Escolher o stop e a simulacao conservadora.
        close_long(store, agent, stop, execution_time_ms, "STOP_AMBOS_NIVEIS")
    elif stop_touched:
        close_long(store, agent, stop, execution_time_ms, "STOP")
    else:
        close_long(store, agent, target, execution_time_ms, "ALVO")


def apply_forecast_outcome(agent: dict[str, Any], candle: dict[str, Any]) -> None:
    forecast = agent.get("forecast")
    if not forecast:
        return
    label = int(candle["close"] > candle["open"])
    probability = float(forecast["probability"])
    predicted = int(probability >= 0.5)
    correct = predicted == label
    if correct:
        agent["prediction_correct"] += 1
    else:
        agent["prediction_error"] += 1
    features = forecast["features"]
    error = label - probability
    rate = float(agent["learning_rate"])
    agent["bias"] = clamp(agent["bias"] + rate * error, -4.0, 4.0)
    agent["weights"] = [
        clamp(weight + rate * error * value, -5.0, 5.0)
        for weight, value in zip(agent["weights"], features)
    ]
    agent["forecast"] = None


def choose_action(agent: dict[str, Any], probability: float) -> str:
    if agent["status"] != "ATIVA":
        return "FALIDA"
    if agent["btc"] > 0:
        return "SAIR" if probability < 0.5 else "MANTER"
    if probability >= agent["entry_threshold"]:
        return "COMPRAR"
    return "AGUARDAR"


def evaluate_generation(agents: list[dict[str, Any]], price: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ranked = sorted(agents, key=lambda agent: agent_fitness(agent, price), reverse=True)
    summary = {
        "quantidade": len(ranked),
        "ativas": sum(agent["status"] == "ATIVA" for agent in ranked),
        "falidas": sum(agent["status"] == "FALIDA" for agent in ranked),
        "melhor_id": ranked[0]["id"] if ranked else None,
        "melhor_fitness": agent_fitness(ranked[0], price) if ranked else None,
        "melhor_equity_usdt": ranked[0].get("equity", INITIAL_BANK) if ranked else None,
        "melhor_pontos": (
            {"acertos": ranked[0]["points_correct"], "erros": ranked[0]["points_error"]}
            if ranked else None
        ),
        "agentes": [
            {
                "id": item["id"],
                "parent_id": item.get("parent_id"),
                "equity_usdt": item.get("equity", INITIAL_BANK),
                "retorno_pct": (safe_float(item.get("equity"), INITIAL_BANK) / INITIAL_BANK - 1) * 100,
                "drawdown_max_pct": safe_float(item.get("max_drawdown")) * 100,
                "fitness": agent_fitness(item, price),
                "acertos": item["points_correct"],
                "erros": item["points_error"],
                "trades": item["trade_count"],
            }
            for item in ranked
        ],
    }
    return ranked, summary


class EvolutionEngine:
    def __init__(self, store: Store, agents: list[dict[str, Any]], generation: int, population_size: int):
        self.store = store
        self.agents = agents
        self.generation = generation
        self.population_size = population_size
        self.generation_candles = int(store.get_meta("generation_candles", 0))
        self.last_processed = int(store.get_meta("last_processed_open_time", 0))
        self.last_price = 0.0
        self.last_signal = "Aguardando primeiro candle fechado"
        self.last_signal = str(store.get_meta("last_signal", self.last_signal))
        self.last_signal_time = str(store.get_meta("last_signal_time", "—"))
        self.websocket_open = False
        self.error_message = ""
        self.training_metrics = store.get_meta("training_metrics", {})
        self.latest_candle: dict[str, Any] | None = None
        self.last_snapshot = 0.0
        self.last_state_save = time.monotonic()
        self.population_next = int(store.get_meta("population_next", population_size))

    def apply_live_update(self, candle: dict[str, Any]) -> None:
        self.latest_candle = candle
        self.last_price = candle["close"]
        execution_time_ms = int(time.time() * 1000)
        for agent in self.agents:
            if agent["status"] != "ATIVA":
                continue
            execute_pending(self.store, agent, candle, execution_time_ms)
            check_intrabar(self.store, agent, candle, execution_time_ms)
            update_equity(agent, candle["close"])
        now = time.time()
        if now - self.last_snapshot >= SNAPSHOT_SECONDS:
            self.store.save_snapshot(candle, int(now * 1000), self.websocket_open)
            self.last_snapshot = now
        if now - self.last_state_save >= SAVE_SECONDS:
            self.persist()
            self.last_state_save = now

    def process_closed(
        self,
        candle: dict[str, Any],
        candles: list[dict[str, Any]],
        rows: list[list[float] | None],
        feature_index: int | None = None,
    ) -> None:
        if candle["open_time"] <= self.last_processed:
            return
        for agent in self.agents:
            if agent["status"] == "ATIVA":
                execute_pending(self.store, agent, candle, candle["open_time"])
                check_intrabar(self.store, agent, candle, candle["close_time"])
                apply_forecast_outcome(agent, candle)
                update_equity(agent, candle["close"])
        self.last_price = candle["close"]
        self.latest_candle = candle
        index = len(candles) - 1 if feature_index is None else feature_index
        features = rows[index] if index < len(rows) else None
        if features is not None:
            for agent in self.agents:
                if agent["status"] != "ATIVA":
                    continue
                probability = predict_probability(agent["weights"], agent["bias"], features)
                action = choose_action(agent, probability)
                agent["last_probability"] = probability
                agent["last_action"] = action
                agent["forecast"] = {
                    "features": features,
                    "probability": probability,
                    "candle_open_time": candle["open_time"],
                }
                if action in ("COMPRAR", "SAIR"):
                    agent["pending_action"] = action
                    agent["pending_after_open_ms"] = candle["open_time"]
                else:
                    agent["pending_action"] = None
                    agent["pending_after_open_ms"] = None
                update_equity(agent, candle["close"])
                self.store.save_decision(candle, agent, features, action)
            best = max(
                (agent for agent in self.agents if agent["status"] == "ATIVA"),
                key=lambda agent: agent_fitness(agent, candle["close"]),
                default=None,
            )
            if best:
                self.last_signal = (
                    f"IA #{best['id']}: {action_label(choose_action(best, best['last_probability']))} "
                    f"(chance estimada de alta {best['last_probability'] * 100:.1f}%)"
                )
                self.last_signal_time = utc_text(candle["close_time"])
                self.store.set_meta("last_signal", self.last_signal)
                self.store.set_meta("last_signal_time", self.last_signal_time)
        self.last_processed = candle["open_time"]
        self.generation_candles += 1
        self.store.set_meta("last_processed_open_time", self.last_processed)
        self.store.set_meta("generation_candles", self.generation_candles)
        for agent in self.agents:
            self.store.save_agent(agent)
        if self.generation_candles >= GENERATION_CANDLES or not any(
            agent["status"] == "ATIVA" for agent in self.agents
        ):
            self.finish_generation(candle["close"])
        self.store.db.commit()

    def finish_generation(self, price: float) -> None:
        for agent in self.agents:
            if agent["btc"] > 0:
                close_long(self.store, agent, price, int(time.time() * 1000), "FIM_DA_GERACAO")
                update_equity(agent, price)
        ranked, summary = evaluate_generation(self.agents, price)
        if not ranked:
            return
        best = ranked[0]
        fitness = agent_fitness(best, price)
        global_fitness = safe_float(self.store.get_meta("global_best_fitness", -999.0), -999.0)
        if fitness > global_fitness:
            self.store.set_meta("global_best_fitness", fitness)
            self.store.set_meta("global_best_genome", genome_of(best))
            self.store.set_meta("global_best_agent_id", best["id"])
            self.store.set_meta("global_best_generation", self.generation)
            self.store.set_meta("global_best_metrics", {
                "fitness": fitness,
                "points_correct": best["points_correct"],
                "points_error": best["points_error"],
                "trade_count": best["trade_count"],
                "generation": self.generation,
            })
            global_fitness = fitness
        reason = (
            "POPULACAO_FALIU"
            if not any(agent["status"] == "ATIVA" for agent in self.agents)
            else f"JANELA_{GENERATION_DAYS}_DIAS"
        )
        self.store.db.execute(
            "UPDATE generations SET ended_utc=?,reason=?,summary_json=? WHERE generation=?",
            (utc_text(), reason, json_text(summary), self.generation),
        )
        for agent in self.agents:
            agent["status"] = "ARQUIVADA" if agent["status"] == "ATIVA" else "FALIDA"
            self.store.save_agent(agent)
        self.store.event("GERACAO_AVALIADA", summary, self.generation)
        champion = self.store.get_meta("global_best_genome")
        parent_candidates = ranked[: max(1, math.ceil(len(ranked) * 0.5))]
        if champion:
            champion_metrics = self.store.get_meta("global_best_metrics", {})
            champion_agent = {
                "id": self.store.get_meta("global_best_agent_id"),
                **champion,
                **champion_metrics,
                "fitness": global_fitness,
            }
            parent_candidates.append(champion_agent)
        next_size = self.population_next
        self.generation += 1
        self.population_size = next_size
        self.agents = new_population(self.store, self.generation, next_size, parent_candidates)
        self.generation_candles = 0
        self.store.set_meta("population_size", next_size)
        self.store.set_meta("population_next", next_size)
        self.store.set_meta("current_generation", self.generation)
        self.store.set_meta("generation_candles", 0)
        self.store.set_meta("generation_start_utc", utc_text())
        self.store.db.commit()
        self.last_signal = (
            f"Geração {self.generation} iniciada; melhor anterior: IA #{best['id']} "
            f"(fitness {fitness:+.4f})"
        )
        self.last_signal_time = utc_text()
        self.store.set_meta("last_signal", self.last_signal)
        self.store.set_meta("last_signal_time", self.last_signal_time)

    def persist(self) -> None:
        for agent in self.agents:
            self.store.save_agent(agent)
        self.store.set_meta("current_generation", self.generation)
        self.store.set_meta("population_size", self.population_size)
        self.store.set_meta("population_next", self.population_next)
        self.store.set_meta("generation_candles", self.generation_candles)
        self.store.set_meta("last_processed_open_time", self.last_processed)
        self.store.set_meta("last_signal", self.last_signal)
        self.store.set_meta("last_signal_time", self.last_signal_time)
        self.store.db.commit()


def action_label(action: str) -> str:
    return {
        "COMPRAR": "COMPRA SIMULADA",
        "SAIR": "SAIR PARA USDT",
        "MANTER": "MANTER POSIÇÃO",
        "AGUARDAR": "AGUARDAR",
        "FALIDA": "SEM BANCA",
    }.get(action, action)


def process_new_historical(store: Store, engine: EvolutionEngine, new_candles: list[dict[str, Any]]) -> None:
    if not new_candles:
        return
    all_candles = store.candle_rows()
    rows = feature_rows(all_candles)
    index_by_open = {candle["open_time"]: index for index, candle in enumerate(all_candles)}
    for candle in new_candles:
        index = index_by_open.get(candle["open_time"])
        if index is None:
            continue
        engine.process_closed(candle, all_candles, rows, index)


def format_time_left(server_ms: int) -> str:
    next_close = ((server_ms // INTERVAL_MS) + 1) * INTERVAL_MS
    left = max(0, (next_close - server_ms) // 1000)
    return f"{left // 60:02d}:{left % 60:02d}"


def render_status(engine: EvolutionEngine, store: Store, server_offset_ms: int) -> None:
    try:
        import os
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass
    server_ms = int(time.time() * 1000) + server_offset_ms
    price = engine.last_price
    alive = [agent for agent in engine.agents if agent["status"] == "ATIVA"]
    ranked = sorted(alive, key=lambda agent: agent_fitness(agent, price), reverse=True)
    champion_id = store.get_meta("global_best_agent_id")
    champion_fitness = safe_float(store.get_meta("global_best_fitness", 0.0), 0.0)
    pending_size = engine.population_next
    line = "=" * 78
    print(line)
    print("IA EVOLUTIVA AGRESSIVA BTCUSDT | PAPER TRADING | CANDLES DE 15 MINUTOS")
    print(line)
    print(f"Estado do monitor: {'CONECTADO' if engine.websocket_open else 'RECONECTANDO/AGUARDANDO'}")
    print(f"Hora UTC: {utc_text(server_ms)} | Próximo fechamento 15m em: {format_time_left(server_ms)}")
    print(f"Preço BTCUSDT: {price:,.2f} USDT" if price else "Preço BTCUSDT: carregando...")
    print(
        f"Geração: {engine.generation} | candles nesta geração: "
        f"{engine.generation_candles}/{GENERATION_CANDLES} ({GENERATION_DAYS} dias)"
    )
    print(
        f"População: {len(engine.agents)} IAs | ativas: {len(alive)} | "
        f"banca inicial: {INITIAL_BANK:.2f} USDT por IA"
    )
    print(f"Total fictício da população no início: {len(engine.agents) * INITIAL_BANK:,.2f} USDT")
    print(f"Tamanho pedido para a próxima geração: {pending_size}")
    if champion_id is None:
        print("Melhor estratégia histórica: ainda não há geração concluída")
    else:
        print(f"Melhor estratégia histórica: IA #{champion_id} | fitness {champion_fitness:+.4f}")
    champion_metrics = store.get_meta("global_best_metrics", {})
    if champion_id is not None and champion_metrics:
        print(
            "Pontuação herdada do melhor: "
            f"{champion_metrics.get('points_correct', 0)} acertos / "
            f"{champion_metrics.get('points_error', 0)} erros em "
            f"{champion_metrics.get('trade_count', 0)} trades"
        )
    print(f"Último sinal do líder: {engine.last_signal} | {engine.last_signal_time}")
    print(
        f"Custos simulados por lado: taxa {FEE_BPS_PER_SIDE:.1f} bps + slippage "
        f"{SLIPPAGE_BPS_PER_SIDE:.1f} bps"
    )
    print("Perfil: entradas mais frequentes (limiar 50–65%) | risco por trade de 1–6% da banca")
    print("Modo: Spot comprado; sinal de baixa fecha a posição ou mantém USDT. Sem short/alavancagem.")
    print(f"Memória persistente: {DB_PATH}")
    if engine.error_message:
        print(f"Aviso de conexão: {engine.error_message[:160]}")
    if engine.training_metrics:
        accuracy = engine.training_metrics.get("acuracia_holdout")
        accuracy_text = f"{accuracy * 100:.1f}%" if isinstance(accuracy, (int, float)) else "n/d"
        print(
            f"Treino histórico inicial: {engine.training_metrics.get('amostras_treino', 0):,} "
            f"amostras | holdout cronológico: {accuracy_text} "
            f"({engine.training_metrics.get('amostras_holdout', 0):,} amostras; só referência)"
        )
    print(line)
    print(" ID  AÇÃO                 SALDO   PATRIMÔNIO  RETORNO  A/E TRADE  P(ALTA) LIMIAR RISCO")
    for agent in ranked[:15]:
        equity = agent["cash"] + agent["btc"] * price
        return_pct = (equity / INITIAL_BANK - 1.0) * 100.0
        print(
            f"{agent['id']:>3}  {action_label(agent.get('last_action', 'AGUARDAR')):<20} "
            f"{agent['cash']:>7.2f}  {equity:>10.2f} {return_pct:>+7.2f}%  "
            f"{agent['points_correct']:>3}/{agent['points_error']:<3}  "
            f"{agent['last_probability'] * 100:>5.1f}% {agent['entry_threshold'] * 100:>5.1f}% "
            f"{agent['risk_per_trade'] * 100:>4.1f}%"
        )
    if len(ranked) > 15:
        print(f"... mais {len(ranked) - 15} agentes ativas (todos recebem e analisam os mesmos candles)")
    if not ranked:
        print("Nenhuma IA ativa; a próxima geração começa quando o candle atual for processado.")
    print(line)
    print("Pressione Ctrl+C para salvar e sair. Na próxima abertura, a memória será retomada.")


def start_websocket(events: queue.Queue, stop_event: threading.Event) -> threading.Thread:
    def worker() -> None:
        delay = 1.0
        while not stop_event.is_set():
            opened = threading.Event()
            def on_open(_ws: Any) -> None:
                nonlocal delay
                delay = 1.0
                opened.set()
                events.put(("open", None))
            def on_message(_ws: Any, message: str) -> None:
                try:
                    payload = json.loads(message)
                    candle = normalize_ws_kline(payload)
                    if candle:
                        events.put(("kline", candle))
                except Exception as error:
                    events.put(("error", f"Mensagem WebSocket inválida: {error}"))
            def on_error(_ws: Any, error: Any) -> None:
                events.put(("error", f"WebSocket: {error}"))
            def on_close(_ws: Any, code: Any, reason: Any) -> None:
                events.put(("close", f"Conexão encerrada ({code}: {reason})"))
            app = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            app.run_forever(ping_interval=20, ping_timeout=10)
            if not stop_event.is_set():
                stop_event.wait(delay)
                delay = min(delay * 2, 30.0)
    thread = threading.Thread(target=worker, name="btc-15m-websocket", daemon=True)
    thread.start()
    return thread


def choose_resume_population(store: Store) -> int:
    saved_size = store.get_meta("population_size")
    next_size = ask_population(int(saved_size) if saved_size else None)
    if saved_size is None:
        store.set_meta("population_size", next_size)
        store.set_meta("population_next", next_size)
    elif next_size != int(saved_size):
        store.set_meta("population_next", next_size)
        print(f"População {next_size} será aplicada na próxima geração.")
    store.db.commit()
    return int(saved_size or next_size)


def main() -> int:
    if websocket is None:
        print("Falta a dependência websocket-client.")
        print("Execute o arquivo 'Abrir IA Evolutiva BTC 15m.bat' para instalar automaticamente.")
        return 2
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    store = Store(DB_PATH)
    population_size = choose_resume_population(store)
    try:
        if store.get_meta("configuracao_inicial") is None:
            store.set_meta("configuracao_inicial", {
                "par": SYMBOL,
                "intervalo": INTERVAL,
                "historico_inicial_dias": HISTORY_DAYS,
                "geracao_dias": GENERATION_DAYS,
                "banca_por_ia_usdt": INITIAL_BANK,
                "taxa_bps_por_lado": FEE_BPS_PER_SIDE,
                "slippage_bps_por_lado": SLIPPAGE_BPS_PER_SIDE,
                "perfil_entradas": "agressivo",
                "limiar_entrada_minimo": 0.50,
                "limiar_entrada_maximo": 0.65,
                "risco_por_trade_minimo": 0.01,
                "risco_por_trade_maximo": 0.06,
                "features": list(FEATURE_NAMES),
                "versao_modelo": "logistic-online-genetica-agressiva-v1",
            })
            store.db.commit()
        print("Consultando dados públicos da Binance e sincronizando candles BTCUSDT de 15m...")
        server_time, downloaded = sync_history(store, HISTORY_DAYS)
        server_offset_ms = server_time - int(time.time() * 1000)
        candles = store.candle_rows()
        if len(candles) < 60:
            raise RuntimeError("A Binance retornou poucos candles para iniciar a aprendizagem.")
        if store.get_meta("current_generation") is None:
            print(f"Treinando modelo inicial com histórico disponível ({len(candles):,} candles)...")
            agents = initialize_population(store, candles, population_size)
            generation = 1
            last_processed = int(store.get_meta("last_processed_open_time", candles[-1]["open_time"]))
            print(
                f"Modelo-base treinado. A população inicial tem {len(agents)} IAs; "
                "holdout cronológico salvo como referência."
            )
        else:
            generation = int(store.get_meta("current_generation"))
            agents = store.load_agents(generation)
            last_processed = int(store.get_meta("last_processed_open_time", candles[-1]["open_time"]))
            if not agents:
                raise RuntimeError("A memória indica uma geração, mas não há agentes salvos.")
            print(
                f"Memória retomada: geração {generation}, {len(agents)} IAs e "
                f"{store.db.execute('SELECT COUNT(*) FROM trades').fetchone()[0]:,} trades registrados."
            )
        engine = EvolutionEngine(store, agents, generation, population_size)
        engine.last_processed = last_processed
        engine.last_price = candles[-1]["close"]
        engine.latest_candle = candles[-1]
        missed = [item for item in candles if item["open_time"] > last_processed]
        if missed:
            print(f"Recalculando {len(missed)} candles fechados enquanto o programa estava desligado...")
            process_new_historical(store, engine, missed)
        events: queue.Queue = queue.Queue()
        stop_event = threading.Event()
        start_websocket(events, stop_event)
        print("Conectando ao fluxo ao vivo. Nenhuma ordem real será enviada.")
        last_render = 0.0
        last_rest_sync = time.monotonic()
        latest_rows = feature_rows(store.candle_rows())
        latest_candles = store.candle_rows()
        while True:
            try:
                event_type, value = events.get(timeout=0.5)
                if event_type == "open":
                    engine.websocket_open = True
                    engine.error_message = ""
                elif event_type == "close":
                    engine.websocket_open = False
                    engine.error_message = str(value)
                elif event_type == "error":
                    engine.error_message = str(value)
                elif event_type == "kline":
                    engine.websocket_open = True
                    engine.apply_live_update(value)
                    if value.get("closed"):
                        store.insert_candle(value)
                        store.db.commit()
                        latest_candles = store.candle_rows()
                        latest_rows = feature_rows(latest_candles)
                        engine.process_closed(value, latest_candles, latest_rows)
            except queue.Empty:
                pass
            if time.monotonic() - last_rest_sync >= 60:
                try:
                    server_time, _ = sync_history(store, HISTORY_DAYS, quiet=True)
                    server_offset_ms = server_time - int(time.time() * 1000)
                    missing = store.candles_after(engine.last_processed)
                    if missing:
                        latest_candles = store.candle_rows()
                        latest_rows = feature_rows(latest_candles)
                        process_new_historical(store, engine, missing)
                    last_rest_sync = time.monotonic()
                except Exception as error:
                    engine.error_message = f"Sincronização REST: {error}"
                    last_rest_sync = time.monotonic()
            if time.monotonic() - last_render >= 1.0:
                render_status(engine, store, server_offset_ms)
                last_render = time.monotonic()
    except KeyboardInterrupt:
        print("\nSalvando memória da população...")
        if "engine" in locals():
            engine.persist()
        store.db.commit()
        store.close()
        print("Memória salva. Ao abrir novamente, a população poderá continuar de onde parou.")
        return 0
    except Exception as error:
        if "engine" in locals():
            try:
                engine.persist()
            except Exception:
                pass
        store.event("ERRO_FATAL", {"erro": str(error)})
        store.db.commit()
        store.close()
        print(f"Não foi possível iniciar a IA evolutiva: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
