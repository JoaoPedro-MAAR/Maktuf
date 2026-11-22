#!/usr/bin/env python3
"""Gera os gráficos da Etapa 3 a partir dos resultados do runner.

Uso:
    python scripts/graficos.py [-dados data/etapa3] [-saida docs/relatorio/graficos]

Para cada cenário, usa a execução mais recente em <dados>/<cenario>-<data>/ e
lê o metricas.csv (t_s,processo,metrica,rotulos,valor). Os percentis seguem a
mesma regra do runner (e do histogram_quantile do Prometheus): diferença dos
buckets na janela e interpolação linear dentro do bucket.
"""

import argparse
import csv
import glob
import json
import math
import os
import re
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Paleta de referência (categorical light) e tintas de texto.
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e4e3df"
SURFACE = "#fcfcfb"

plt.rcParams.update({
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID,
    "axes.labelcolor": INK2,
    "axes.titlecolor": INK,
    "axes.titlesize": 12,
    "axes.titleweight": "bold",
    "axes.titlelocation": "left",
    "axes.labelsize": 10,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "xtick.color": INK2,
    "ytick.color": INK2,
    "legend.frameon": False,
    "legend.labelcolor": INK2,
    "lines.linewidth": 2,
    "font.size": 10,
})


# ---------------------------------------------------------------- leitura

class Run:
    def __init__(self, path):
        self.path = path
        self.name = re.sub(r"-\d{8}-\d{6}\.\d+$", "", os.path.basename(path))
        # (processo, metrica) -> lista de (t, rotulos(dict), valor)
        self.rows = defaultdict(list)
        with open(os.path.join(path, "metricas.csv"), newline="") as f:
            for r in csv.DictReader(f):
                if r.get("valor") is None:  # linha truncada (execução interrompida)
                    continue
                labels = dict(p.split("=", 1) for p in r["rotulos"].split(",") if "=" in p)
                self.rows[(r["processo"], r["metrica"])].append(
                    (float(r["t_s"]), labels, float(r["valor"])))
        self.resumo = []
        rp = os.path.join(path, "resumo.csv")
        if os.path.exists(rp):
            with open(rp, newline="") as f:
                self.resumo = list(csv.DictReader(f))

    def processes(self, prefix):
        return sorted({p for p, _ in self.rows if p.startswith(prefix)})

    def series(self, proc, metric, **want):
        """Soma, por instante, as séries que batem com os rótulos pedidos."""
        acc = defaultdict(float)
        for t, labels, v in self.rows.get((proc, metric), []):
            if all(labels.get(k) == w for k, w in want.items()):
                acc[t] += v
        ts = sorted(acc)
        return ts, [acc[t] for t in ts]

    def buckets_at(self, proc, metric, t, **want):
        """Buckets acumulados (le -> contagem) na última coleta <= t."""
        rows = [r for r in self.rows.get((proc, metric + "_bucket"), [])
                if all(r[1].get(k) == w for k, w in want.items())]
        times = sorted({r[0] for r in rows if r[0] <= t})
        if not times:
            return {}
        last = times[-1]
        out = defaultdict(float)
        for tt, labels, v in rows:
            if tt == last:
                le = labels["le"]
                out[math.inf if le == "+Inf" else float(le)] += v
        return out

    def quantile(self, proc, metric, q, t0, t1, **want):
        a = self.buckets_at(proc, metric, t0, **want)
        b = self.buckets_at(proc, metric, t1, **want)
        diff = sorted((le, b[le] - a.get(le, 0.0)) for le in b)
        return estimate_quantile(q, diff)


def estimate_quantile(q, buckets):
    if not buckets:
        return float("nan")
    total = buckets[-1][1]
    if total <= 0:
        return float("nan")
    rank = q * total
    prev_le, prev_c = 0.0, 0.0
    for le, c in buckets:
        if c >= rank:
            if math.isinf(le):
                return prev_le
            if c == prev_c:
                return le
            return prev_le + (le - prev_le) * (rank - prev_c) / (c - prev_c)
        prev_le, prev_c = le, c
    return prev_le


def rate(ts, vs, window):
    """Taxa por segundo numa janela deslizante sobre um contador."""
    out_t, out_v, j = [], [], 0
    for i, t in enumerate(ts):
        while ts[j] < t - window:
            j += 1
        if t - ts[j] > 0:
            out_t.append(t)
            out_v.append((vs[i] - vs[j]) / (t - ts[j]))
    return out_t, out_v


def latest_runs(root):
    runs = {}
    for d in sorted(glob.glob(os.path.join(root, "*-*"))):
        if os.path.exists(os.path.join(d, "metricas.csv")):
            name = re.sub(r"-\d{8}-\d{6}\.\d+$", "", os.path.basename(d))
            runs[name] = d  # ordenado por data: fica o mais recente
    return runs


# ---------------------------------------------------------------- desenho

def consumer(run):
    return run.processes("consumidor:")[0]


def goal_line(ax, y, text):
    ax.axhline(y, color=INK2, linestyle=(0, (4, 3)), linewidth=1.2)
    ax.annotate(text, xy=(1, y), xycoords=("axes fraction", "data"), xytext=(-4, 4),
                textcoords="offset points", ha="right", va="bottom", color=INK2, fontsize=9)


def legend_below(ax, fontsize=9):
    n = len(ax.get_legend_handles_labels()[0])
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=min(n, 3), fontsize=fontsize)


def event_line(ax, x, text):
    ax.axvline(x, color=MUTED, linestyle=(0, (2, 2)), linewidth=1.2)
    ax.annotate(text, xy=(x, 1), xycoords=("data", "axes fraction"), xytext=(4, -4),
                textcoords="offset points", ha="left", va="top", color=INK2, fontsize=9)


def save(fig, out, name):
    fig.tight_layout()
    path = os.path.join(out, name)
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print("gerado", path)


def windowed_quantile(run, proc, q, step, t_end, group):
    xs, ys = [], []
    t = step
    while t <= t_end + 1e-9:
        v = run.quantile(proc, "end_to_end_latency_seconds", q, t - step, t, group=group)
        if not math.isnan(v):
            xs.append(t)
            ys.append(v)
        t += step
    return xs, ys


def scenario_duration(run):
    return max(t for rows in run.rows.values() for t, _, _ in rows)


def plot_referencia(run, out):
    c = consumer(run)
    end = scenario_duration(run)
    fig, ax = plt.subplots(figsize=(8, 3.6))
    for q, color, lab in ((0.5, AQUA, "p50"), (0.95, BLUE, "p95"), (0.99, ORANGE, "p99")):
        xs, ys = windowed_quantile(run, c, q, 30, end, "sensor")
        ax.plot([x / 60 for x in xs], ys, color=color, label=lab)
    goal_line(ax, 5, "RNF-01: p95 ≤ 5 s")
    ax.set(xlabel="tempo (min)", ylabel="latência fim a fim (s)",
           title="Referência: latência fim a fim por janela de 30 s")
    ax.set_ylim(bottom=0)
    legend_below(ax)
    save(fig, out, "referencia_latencia.png")

    fig, ax = plt.subplots(figsize=(8, 3.6))
    ts, vs = run.series("sensores:sensor", "sensor_readings_total")
    rt, rv = rate(ts, vs, 30)
    ax.plot([x / 60 for x in rt], rv, color=MUTED, label="oferecida (medidas pelos sensores)")
    ts, vs = run.series(c, "messages_received_total")
    rt, rv = rate(ts, vs, 30)
    ax.plot([x / 60 for x in rt], rv, color=BLUE, label="entregue ao consumidor")
    ax.set(xlabel="tempo (min)", ylabel="leituras/s",
           title="Referência: vazão oferecida e entregue (média móvel de 30 s)")
    ax.set_ylim(bottom=0)
    legend_below(ax)
    save(fig, out, "referencia_vazao.png")

    fig, ax = plt.subplots(figsize=(8, 3.2))
    for proc, color in (("b1", BLUE),):
        ts, vs = run.series(proc, "broker_queue_depth")
        ax.plot([x / 60 for x in ts], vs, color=color, linewidth=1.5)
    ax.set(xlabel="tempo (min)", ylabel="leituras na fila",
           title="Referência: fila do assinante no primário (b1)")
    ax.set_ylim(bottom=0)
    save(fig, out, "referencia_fila.png")


def escala_points(runs, pattern):
    pts = []
    for name, run in runs.items():
        m = re.fullmatch(pattern, name)
        if not m:
            continue
        n = int(m.group(1))
        c = consumer(run)
        t0, t1 = 60, 180

        def delta(proc, metric, run=run):
            ts, vs = run.series(proc, metric)
            at = lambda t: max((v for tt, v in zip(ts, vs) if tt <= t), default=0)  # noqa: E731
            return (at(t1) - at(t0)) / (t1 - t0)

        offered = delta("sensores:sensor", "sensor_readings_total")
        delivered = delta(c, "messages_received_total")
        p95 = run.quantile(c, "end_to_end_latency_seconds", 0.95, t0, t1, group="sensor")
        p99 = run.quantile(c, "end_to_end_latency_seconds", 0.99, t0, t1, group="sensor")
        _, pend = run.series("sensores:sensor", "sensor_buffer_pending")
        nominal = n * (0.1 + 0.9 / 300)  # mix do perfil: 10% a 1/s, 90% a 1/5 min
        pts.append((n, nominal, delivered, p95, p99, max(pend, default=0), offered))
    return sorted(pts)


def plot_escala(runs, out):
    families = [
        ("com réplica (b1 + b2 + Juiz)", BLUE, escala_points(runs, r"escala_(\d+)")),
        ("sem réplica (só b1)", ORANGE, escala_points(runs, r"escala_sem_replica_(\d+)")),
    ]
    families = [f for f in families if f[2]]
    if not families:
        return {}
    marker = dict(marker="o", markersize=7, markeredgecolor=SURFACE, markeredgewidth=2)

    fig, ax = plt.subplots(figsize=(7, 4.2))
    allp = [p for _, _, pts in families for p in pts]
    lim = max(max(p[1] for p in allp), max(p[2] for p in allp)) * 1.1
    ax.plot([0, lim], [0, lim], color=MUTED, linewidth=1, linestyle=(0, (4, 3)))
    ax.annotate("entrega = oferta", xy=(lim * 0.55, lim * 0.55), xytext=(-8, 6),
                textcoords="offset points", color=MUTED, fontsize=9, ha="right", rotation=0)
    for label, color, pts in families:
        ax.plot([p[1] for p in pts], [p[2] for p in pts], color=color, label=label, **marker)
    for n, off, dl, *_ in families[0][2]:
        ax.annotate(f"{n}", xy=(off, dl), xytext=(0, -16), textcoords="offset points",
                    color=INK2, fontsize=8, ha="center")
    ax.set(xlim=(0, lim), ylim=(0, lim), xlabel="carga oferecida nominal (leituras/s)",
           ylabel="vazão entregue (leituras/s)",
           title="Escala: vazão entregue × oferecida (rótulo = sensores)")
    legend_below(ax)
    save(fig, out, "escala_vazao.png")

    fig, ax = plt.subplots(figsize=(7, 4.2))
    for label, color, pts in families:
        offs = [p[1] for p in pts]
        ax.plot(offs, [p[3] for p in pts], color=color, label=f"p95 {label}", **marker)
        ax.plot(offs, [p[4] for p in pts], color=color, linestyle=(0, (3, 2)), linewidth=1.5,
                label=f"p99 {label}")
    goal_line(ax, 5, "RNF-01: p95 ≤ 5 s")
    ax.set_yscale("log")
    ax.set(xlabel="carga oferecida nominal (leituras/s)", ylabel="latência fim a fim (s, escala log)",
           title="Escala: latência × carga oferecida")
    legend_below(ax, fontsize=8)
    save(fig, out, "escala_latencia.png")
    return {label: pts for label, _, pts in families}


def plot_justica(run, out):
    c = consumer(run)
    end = scenario_duration(run)
    fig, ax = plt.subplots(figsize=(8, 3.6))
    xs, ys = windowed_quantile(run, c, 0.95, 10, end, "sensor")
    ax.plot(xs, ys, color=BLUE, label="p95 dos sensores normais")
    ts, vs = run.series(c, "messages_received_total", group="agressivo")
    event_line(ax, 60, "agressivos entram (3 × 1 ms)")
    base = run.quantile(c, "end_to_end_latency_seconds", 0.95, 10, 60, group="sensor")
    goal_line(ax, 2 * base + 0.5, f"RNF-05: 2 × base + 0,5 s ({2 * base + 0.5:.2f} s)")
    ax.set(xlabel="tempo (s)", ylabel="latência p95 (s)",
           title="Justiça: p95 dos sensores normais por janela de 10 s")
    ax.set_ylim(0, (2 * base + 0.5) * 1.35)
    legend_below(ax)
    save(fig, out, "justica_latencia.png")

    fig, ax = plt.subplots(figsize=(8, 3.2))
    for group, color, lab in (("sensor", BLUE, "20 normais"), ("agressivo", ORANGE, "3 agressivos")):
        ts, vs = run.series(c, "messages_received_total", group=group)
        rt, rv = rate(ts, vs, 5)
        ax.plot(rt, rv, color=color, label=lab)
    event_line(ax, 60, "agressivos entram")
    ax.set(xlabel="tempo (s)", ylabel="leituras/s entregues",
           title="Justiça: vazão entregue por grupo (média móvel de 5 s)")
    ax.set_ylim(bottom=0)
    legend_below(ax)
    save(fig, out, "justica_vazao.png")


def fault_times(run):
    """Instantes das falhas aplicadas, lidos do relatorio.md do runner."""
    rp = os.path.join(run.path, "relatorio.md")
    out = []
    if os.path.exists(rp):
        for line in open(rp):
            # | Previsto | Real | Ação | Alvo | Processo | Erro |
            m = re.match(r"\|\s*\S+\s*\|\s*([\d.]+)s\s*\|\s*(kill|stop|start)\s*\|\s*\S+\s*\|\s*(\S+)\s*\|",
                         line)
            if m:
                out.append((m.group(2), m.group(3), float(m.group(1))))
    return out


def plot_falha(run, out, fname, title, default_fault):
    c = consumer(run)
    faults = fault_times(run) or [("kill", "", default_fault)]
    fig, (ax, ax2) = plt.subplots(2, 1, figsize=(8, 5), sharex=True,
                                  gridspec_kw={"height_ratios": [3, 1.4]})
    ts, vs = run.series(c, "messages_received_total")
    rt, rv = rate(ts, vs, 2)
    ax.plot(rt, rv, color=BLUE, label="entregues ao consumidor")
    ts, vs = run.series("sensores:sensor", "sensor_readings_confirmed_total")
    rt, rv = rate(ts, vs, 2)
    ax.plot(rt, rv, color=AQUA, label="confirmadas aos sensores (PUBACK)")
    for action, target, t in faults:
        event_line(ax, t, f"{action} -9 {target}".strip() if action == "kill" else f"{action} {target}")
    ax.set(ylabel="leituras/s (janela de 2 s)", title=title)
    ax.set_ylim(0, ax.get_ylim()[1] * 1.15)
    ax.legend(loc="lower center", ncol=2)

    ts, vs = run.series("sensores:sensor", "sensor_buffer_pending")
    ax2.plot(ts, vs, color=ORANGE, linewidth=1.5)
    for _, _, t in faults:
        ax2.axvline(t, color=MUTED, linestyle=(0, (2, 2)), linewidth=1.2)
    ax2.set(xlabel="tempo (s)", ylabel="pendentes\nnos sensores")
    ax2.set_ylim(bottom=0)
    save(fig, out, fname)


def plot_estado_brokers(run, out):
    faults = fault_times(run) or [("kill", "primario", 30)]
    fig, ax = plt.subplots(figsize=(8, 3.2))
    # protocol.BrokerState; 0 também é o valor do Juiz para broker sem contato.
    names = {0: "fora", 1: "primário", 2: "reserva\nsincronizada", 3: "reserva\nsincronizando"}
    for broker, color, off in (("b1", BLUE, 0.04), ("b2", ORANGE, -0.04)):
        ts, vs = run.series("juiz", "judge_broker_state", broker_id=broker)
        ax.step(ts, [v + off for v in vs], where="post", color=color, label=broker)
    for action, target, t in faults:
        event_line(ax, t, f"{action} -9 {target}")
    ax.set_yticks(list(names), [names[k] for k in names])
    ax.set(xlabel="tempo (s)", title="Queda do primário: estado dos brokers visto pelo Juiz")
    ax.legend(loc="upper right")
    save(fig, out, "queda_primario_estados.png")


def plot_rajada(run, out):
    c = consumer(run)
    fig, ax = plt.subplots(figsize=(8, 3.6))
    ts, vs = run.series("sensores:sensor", "sensor_buffer_pending")
    ax.plot(ts, vs, color=ORANGE, label="pendentes nos buffers dos sensores")
    ts, vs = run.series(c, "messages_received_total")
    base = vs[0] if vs else 0
    ax.plot(ts, [v - base for v in vs], color=BLUE, label="entregues ao consumidor (acumulado)")
    ax.set(xlabel="tempo (s)", ylabel="leituras", title="Rajada de 2.000 leituras: absorção pelo buffer")
    ax.set_ylim(bottom=0)
    legend_below(ax)
    save(fig, out, "rajada.png")


def plot_consumidor_lento(run, out):
    c = consumer(run)
    fig, ax = plt.subplots(figsize=(8, 3.6))
    ts, vs = run.series("b1", "broker_queue_depth")
    ax.plot(ts, vs, color=BLUE, label="fila do assinante (b1)")
    ts, vs = run.series("b1", "retention_drops_total")
    ax.plot(ts, vs, color=ORANGE, label="descartes por retenção (acumulado)")
    ax.set(xlabel="tempo (s)", ylabel="leituras",
           title="Consumidor lento: fila limitada e descarte sinalizado (GAP_BEFORE)")
    ax.set_ylim(bottom=0)
    legend_below(ax)
    save(fig, out, "consumidor_lento.png")


def plot_offline_sono(run, out):
    fig, ax = plt.subplots(figsize=(8, 3.6))
    for state, color in (("online", BLUE), ("dormindo", AQUA), ("offline", ORANGE)):
        ts, vs = run.series("b1", "active_sensors", state=state)
        ax.plot(ts, vs, color=color, label=state)
    ax.set(xlabel="tempo (s)", ylabel="sensores", title="Desconexões e sono: estado dos sensores no broker")
    ax.set_ylim(bottom=0)
    legend_below(ax)
    save(fig, out, "offline_sono.png")


def plot_comparacao(antes, depois, out):
    """Antes x depois do ajuste da Etapa 3 (DELIVER_ACK sem fsync)."""
    marker = dict(marker="o", markersize=7, markeredgecolor=SURFACE, markeredgewidth=2)
    series = [("antes (4 fsyncs/leitura)", ORANGE, antes), ("depois (2 fsyncs/leitura)", BLUE, depois)]

    pts = [(lab, color, escala_points(runs, r"escala_(\d+)")) for lab, color, runs in series]
    pts = [p for p in pts if p[2]]
    if len(pts) == 2:
        fig, ax = plt.subplots(figsize=(7, 4.2))
        lim = max(p[1] for _, _, ps in pts for p in ps) * 1.1
        ax.plot([0, lim], [0, lim], color=MUTED, linewidth=1, linestyle=(0, (4, 3)))
        for lab, color, ps in pts:
            ax.plot([p[1] for p in ps], [p[2] for p in ps], color=color, label=lab, **marker)
        ax.set(xlim=(0, lim), ylim=(0, lim), xlabel="carga oferecida nominal (leituras/s)",
               ylabel="vazão entregue (leituras/s)", title="Ajuste: vazão com réplica, antes × depois")
        legend_below(ax)
        save(fig, out, "ajuste_vazao.png")

        fig, ax = plt.subplots(figsize=(7, 4.2))
        for lab, color, ps in pts:
            ax.plot([p[1] for p in ps], [p[3] for p in ps], color=color, label=f"p95 {lab}", **marker)
        goal_line(ax, 5, "RNF-01: p95 ≤ 5 s")
        ax.set_yscale("log")
        ax.set(xlabel="carga oferecida nominal (leituras/s)", ylabel="latência p95 (s, escala log)",
               title="Ajuste: latência p95 com réplica, antes × depois")
        legend_below(ax)
        save(fig, out, "ajuste_latencia.png")

    if "referencia" in antes and "referencia" in depois:
        fig, ax = plt.subplots(figsize=(8, 3.6))
        for lab, color, runs in series:
            run = runs["referencia"]
            xs, ys = windowed_quantile(run, consumer(run), 0.95, 30, scenario_duration(run), "sensor")
            ax.plot([x / 60 for x in xs], ys, color=color, label=f"p95 {lab}")
        goal_line(ax, 5, "RNF-01: p95 ≤ 5 s")
        ax.set_yscale("log")
        ax.set(xlabel="tempo (min)", ylabel="latência p95 (s, escala log)",
               title="Ajuste: perfil de referência, p95 por janela de 30 s")
        legend_below(ax)
        save(fig, out, "ajuste_referencia.png")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-dados", default="data/etapa3")
    ap.add_argument("-saida", default="docs/relatorio/graficos")
    ap.add_argument("-antes", default="data/etapa3-antes",
                    help="resultados de antes do ajuste, para os gráficos de comparação")
    a = ap.parse_args()
    os.makedirs(a.saida, exist_ok=True)

    runs = {n: Run(p) for n, p in latest_runs(a.dados).items()}
    if not runs:
        raise SystemExit(f"nenhum resultado em {a.dados}")
    print("execucoes:", ", ".join(f"{n} ({os.path.basename(r.path)})" for n, r in sorted(runs.items())))

    if "referencia" in runs:
        plot_referencia(runs["referencia"], a.saida)
    pts = plot_escala(runs, a.saida)
    if "justica" in runs:
        plot_justica(runs["justica"], a.saida)
    if "queda_primario" in runs:
        plot_falha(runs["queda_primario"], a.saida, "queda_primario.png",
                   "Queda do primário: vazão durante o failover", 30)
        plot_estado_brokers(runs["queda_primario"], a.saida)
    if "queda_juiz" in runs:
        plot_falha(runs["queda_juiz"], a.saida, "queda_juiz.png",
                   "Queda do Juiz: o fluxo de dados continua", 30)
    if "rajada" in runs:
        plot_rajada(runs["rajada"], a.saida)
    if "consumidor_lento" in runs:
        plot_consumidor_lento(runs["consumidor_lento"], a.saida)
    if "offline_sono" in runs:
        plot_offline_sono(runs["offline_sono"], a.saida)

    if os.path.isdir(a.antes):
        antes = {n: Run(p) for n, p in latest_runs(a.antes).items()}
        if antes:
            plot_comparacao(antes, runs, a.saida)

    resumo = [r for run in runs.values() for r in run.resumo]
    if resumo:
        with open(os.path.join(a.saida, "metas.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(resumo[0]))
            w.writeheader()
            w.writerows(resumo)
    if pts:
        with open(os.path.join(a.saida, "escala.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["serie", "sensores", "oferta_nominal_por_s", "entregue_por_s", "p95_s", "p99_s",
                        "max_pendentes", "medida_por_s"])
            for label, rows in pts.items():
                for p in rows:
                    w.writerow([label, p[0]] + [f"{x:.3f}" for x in p[1:]])
    print(json.dumps({"cenarios": sorted(runs)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
