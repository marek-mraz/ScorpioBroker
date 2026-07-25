#!/usr/bin/env bash
# RAM+CPU sampler + report for the 5-broker ETSI stack (scorpio-* containers via docker stats).
# Usage:
#   dev/mem-monitor.sh sample <csv>            # foreground sampling loop — caller backgrounds it
#   dev/mem-monitor.sh event  <csv> <label>    # mark a suite boundary in the csv
#   dev/mem-monitor.sh report <csv> <outdir>   # memory-report.md + cpu-report.md (min/median/mean/max),
#                                              # memory-usage.png, resource-summary.md + gate-status.txt
#                                              # (gate: scorpio-broker-* peak RSS <= MEM_LIMIT_MB, default 350)
# PYTHON env var picks the interpreter for report (default python3); matplotlib auto-installs if missing.
set -u
case "${1:-}" in
  sample)
    CSV="$2"; : > "$CSV"
    while :; do
      docker stats --no-stream --format '{{.Name}},{{.MemUsage}},{{.CPUPerc}}' 2>/dev/null |
        awk -F'[, ]' -v t="$(date +%s)" '/^scorpio-/{print t","$1","$2","$5}' >> "$CSV"
      sleep 5
    done ;;
  event)
    echo "$(date +%s),EVENT,$3" >> "$2" ;;
  report)
    "${PYTHON:-python3}" - "$2" "$3" <<'EOF'
import csv, os, re, statistics, subprocess, sys
try:
    import matplotlib
except ImportError:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "matplotlib"], check=True)
    import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

MULT = {"B": 1 / 2**20, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024,
        "kB": 1e3 / 2**20, "MB": 1e6 / 2**20, "GB": 1e9 / 2**20}
def mib(s):
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", s)
    return float(m.group(1)) * MULT[m.group(2)] if m and m.group(2) in MULT else None

csvp, outdir = sys.argv[1], sys.argv[2]
series, cpu_series, events = {}, {}, []
for row in csv.reader(open(csvp)):
    if len(row) < 3:
        continue
    if row[1] == "EVENT":
        events.append((int(row[0]), row[2]))
        continue
    v = mib(row[2])
    if v is not None:
        series.setdefault(row[1], []).append((int(row[0]), v))
    if len(row) >= 4 and row[3].endswith("%"):
        try:
            cpu_series.setdefault(row[1], []).append(float(row[3].rstrip("%")))
        except ValueError:
            pass
if not series:
    sys.exit(f"no samples in {csvp}")
t0 = min(t for pts in series.values() for t, _ in pts)

def stats_table(title, data, fmt):
    lines = [title, "",
             "| container | samples | min | median | mean | max |",
             "|---|---|---|---|---|---|"]
    for name in sorted(data):
        v = data[name]
        lines.append(f"| {name} | {len(v)} | {fmt(min(v))} | {fmt(statistics.median(v))}"
                     f" | {fmt(statistics.mean(v))} | {fmt(max(v))} |")
    return "\n".join(lines) + "\n"

open(f"{outdir}/memory-report.md", "w").write(
    stats_table("## RAM usage during ETSI run (MiB)",
                {n: [x for _, x in pts] for n, pts in series.items()}, lambda x: f"{x:.0f}"))
if cpu_series:
    open(f"{outdir}/cpu-report.md", "w").write(
        stats_table("## CPU usage during ETSI run (% of one core)",
                    cpu_series, lambda x: f"{x:.1f}"))

# Resource gate + short summary: scorpio-broker-* peak RSS must stay <= MEM_LIMIT_MB.
LIMIT = float(os.environ.get("MEM_LIMIT_MB", "350"))
broker_peaks = {n: max(x for _, x in pts) for n, pts in series.items()
                if n.startswith("scorpio-broker-")}
breaches = {n: p for n, p in broker_peaks.items() if p > LIMIT}
summary = ["## Resource summary", ""]
if broker_peaks:
    worst = max(broker_peaks, key=broker_peaks.get)
    verdict = "❌ FAIL" if breaches else "✅ PASS"
    summary.append(f"- **Memory gate** (limit **{LIMIT:.0f} MiB** per scorpio broker): {verdict} — "
                   f"highest broker peak **{broker_peaks[worst]:.0f} MiB** ({worst})")
    for n, p in sorted(breaches.items()):
        summary.append(f"  - ❌ {n} peaked at **{p:.0f} MiB** (> {LIMIT:.0f} MiB)")
else:
    summary.append("- **Memory gate**: no scorpio-broker-* samples found — gate not evaluated")
broker_cpu = {n: v for n, v in cpu_series.items() if n.startswith("scorpio-broker-")}
if broker_cpu:
    peak_n = max(broker_cpu, key=lambda n: max(broker_cpu[n]))
    med = statistics.median([x for v in broker_cpu.values() for x in v])
    summary.append(f"- **CPU**: broker peak **{max(broker_cpu[peak_n]):.0f}%** ({peak_n}), "
                   f"median across all broker samples {med:.0f}%")
open(f"{outdir}/resource-summary.md", "w").write("\n".join(summary) + "\n")
open(f"{outdir}/gate-status.txt", "w").write("FAIL" if breaches else "PASS")

# One panel per role (broker/kafka/postgres/mqtt), one line per instance; instance N keeps
# the same color in every panel. Suite boundaries = dotted verticals, labeled in the top panel.
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
ORDER = {"broker": 0, "kafka": 1, "postgres": 2, "mqtt": 3}
def role(n):
    return n.split("-")[1] if "-" in n else n
roles = sorted({role(n) for n in series}, key=lambda r: ORDER.get(r, 9))

fig, axes = plt.subplots(len(roles), 1, figsize=(12, 2.6 * len(roles)),
                         sharex=True, squeeze=False)
axes = axes.flatten()
fig.patch.set_facecolor("#fcfcfb")
for ax, r in zip(axes, roles):
    ax.set_facecolor("#fcfcfb")
    for name in sorted(n for n in series if role(n) == r):
        inst = name.rsplit("-", 1)[-1]
        idx = int(inst) - 1 if inst.isdigit() else 0
        ax.plot([(t - t0) / 60 for t, _ in series[name]], [v for _, v in series[name]],
                color=COLORS[idx % len(COLORS)], lw=2, label=inst)
    ax.set_ylabel(f"{r} (MiB)", color="#0b0b0b")
    ax.set_ylim(bottom=0)
    ax.grid(True, color="#e5e4e1", lw=0.7)
    ax.tick_params(colors="#52514e")
    for s in ax.spines.values():
        s.set_visible(False)
    for t, _ in events:
        ax.axvline((t - t0) / 60, color="#c3c2b7", lw=1, ls=":")
for t, lbl in events:
    axes[0].text((t - t0) / 60, axes[0].get_ylim()[1] * 0.98, lbl.split("/")[-1],
                 rotation=90, va="top", ha="right", fontsize=7, color="#52514e")
h, l = axes[0].get_legend_handles_labels()
fig.legend(h, l, title="instance", ncols=len(l), frameon=False, fontsize=8,
           loc="upper right", bbox_to_anchor=(0.99, 1.0))
axes[-1].set_xlabel("minutes since start", color="#0b0b0b")
fig.suptitle("RAM usage during ETSI run", color="#0b0b0b", x=0.02, ha="left")
fig.tight_layout(rect=(0, 0, 1, 0.965))
fig.savefig(f"{outdir}/memory-usage.png", dpi=140)
print(f"wrote {outdir}/memory-report.md + cpu-report.md + resource-summary.md"
      f" (gate: {'FAIL' if breaches else 'PASS'}) + memory-usage.png")
EOF
    ;;
  *) echo "usage: $0 sample <csv> | event <csv> <label> | report <csv> <outdir>" >&2; exit 2 ;;
esac
