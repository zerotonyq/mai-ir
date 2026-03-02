import os
import re
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / 'task_4' / 'output'
IMG_DIR = ROOT / 'task_6' / 'img'

LIMITS = [1000, 5000, 10000, 20000]

STATS_RE = {
    'Docs': re.compile(r'^Docs:\s*(\d+)\s*$'),
    'Terms': re.compile(r'^Terms:\s*(\d+)\s*$'),
    'Tokens': re.compile(r'^Tokens:\s*(\d+)\s*$'),
    'AvgTokenLen': re.compile(r'^AvgTokenLen:\s*([0-9.]+)\s*$'),
    'InputKB': re.compile(r'^InputKB:\s*([0-9.]+)\s*$'),
    'Elapsed': re.compile(r'^Elapsed:\s*([0-9.]+)\s*$'),
    'SpeedKBps': re.compile(r'^SpeedKBps:\s*([0-9.]+)\s*$'),
}


def ensure_python_deps():
    try:
        import pymongo  # noqa: F401
        import bs4  # noqa: F401
        import matplotlib  # noqa: F401
        return
    except Exception:
        pass
    print("Installing python deps: pymongo, beautifulsoup4, matplotlib")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pymongo", "beautifulsoup4", "matplotlib"])


def run_transfer():
    cmd = [sys.executable, str(ROOT / "task_7" / "transfer_pages_to_articles.py")]
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd, cwd=ROOT)


def ensure_mongo():
    cmd = ["docker", "compose", "up", "-d", "mongo"]
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd, cwd=ROOT)

def ensure_indexer_image():
    cmd = ["docker", "compose", "build", "indexer"]
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd, cwd=ROOT)

def mongo_count_articles():
    from pymongo import MongoClient

    mongo_url = os.environ.get("MONGO_URL", "mongodb://admin:password@localhost:27017/?authSource=admin")
    client = MongoClient(mongo_url)
    return client["infosearch"]["articles"].count_documents({})


def update_report(total_docs: int, rows):
    report_path = ROOT / "task_6" / "REPORT.tex"
    if not report_path.exists():
        print("REPORT.tex not found, skip update")
        return

    content = report_path.read_text(encoding="utf-8")

    def replace_block(text, begin, end, new_block):
        if begin not in text or end not in text:
            return text
        pre, rest = text.split(begin, 1)
        _mid, post = rest.split(end, 1)
        return pre + begin + "\n" + new_block + "\n" + end + post
    docs_line = f"\u0412\u0441\u0435\u0433\u043e \u0441\u043e\u0431\u0440\u0430\u043d\u043e \u0434\u043e\u043a\u0443\u043c\u0435\u043d\u0442\u043e\u0432: \\textbf{{{total_docs}}}."
    content = replace_block(
        content,
        "% AUTO:DOCS_BEGIN",
        "% AUTO:DOCS_END",
        docs_line,
    )

    table_rows = []
    for r in rows:
        table_rows.append(
            f"{int(r['Docs'])} & {int(r['Terms'])} & {int(r['Tokens'])} & "
            f"{r['AvgTokenLen']:.5f} & {r['InputKB']:.0f} & {r['Elapsed']:.5f} & "
            f"{r['SpeedKBps']:.1f}\\\\"
        )
    content = replace_block(
        content,
        "% AUTO:TABLE_BEGIN",
        "% AUTO:TABLE_END",
        "\n".join(table_rows),
    )

    report_path.write_text(content, encoding="utf-8")


def run_indexer(limit: int):
    env = os.environ.copy()
    env['LIMIT'] = str(limit)
    cmd = ['docker', 'compose', '--profile', 'tools', 'run', '--rm', '-e', f'LIMIT={limit}', 'indexer']
    print('Running:', ' '.join(cmd))
    subprocess.check_call(cmd, cwd=ROOT, env=env)


def parse_stats(path: Path):
    stats = {}
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            for key, rgx in STATS_RE.items():
                m = rgx.match(line.strip())
                if m:
                    val = m.group(1)
                    stats[key] = float(val) if '.' in val else int(val)
    missing = [k for k in STATS_RE.keys() if k not in stats]
    if missing:
        raise RuntimeError(f'Missing stats fields: {missing}')
    return stats


def plot_perf(rows):
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    sizes = [r['InputKB'] for r in rows]
    times = [r['Elapsed'] for r in rows]
    speeds = [r['SpeedKBps'] for r in rows]

    plt.figure(figsize=(8, 5))
    plt.plot(sizes, times, marker='o')
    plt.xlabel('Input, KB')
    plt.ylabel('Elapsed, s')
    plt.title('Indexing time vs input size')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(IMG_DIR / 'perf_time_vs_size.png', dpi=150)

    plt.figure(figsize=(8, 5))
    plt.plot(sizes, speeds, marker='o')
    plt.xlabel('Input, KB')
    plt.ylabel('Speed, KB/s')
    plt.title('Indexing speed vs input size')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(IMG_DIR / 'perf_speed.png', dpi=150)


def plot_zipf():
    freq_csv = OUT_DIR / 'freq.csv'
    if not freq_csv.exists():
        raise RuntimeError('freq.csv not found')

    ranks = []
    freqs = []
    zipf = []
    with freq_csv.open('r', encoding='utf-8') as f:
        next(f)
        for line in f:
            parts = line.strip().split(',')
            if len(parts) < 3:
                continue
            ranks.append(int(parts[0]))
            freqs.append(float(parts[1]))
            zipf.append(float(parts[2]))

    plt.figure(figsize=(8, 6))
    plt.loglog(ranks, freqs, label='Empirical')
    plt.loglog(ranks, zipf, label='Zipf')
    plt.xlabel('Rank (log)')
    plt.ylabel('Frequency (log)')
    plt.title('Zipf Law')
    plt.grid(True, which='both', ls='--', alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(IMG_DIR / 'zipf.png', dpi=150)


def main():
    ensure_python_deps()
    ensure_mongo()
    ensure_indexer_image()
    run_transfer()
    rows = []
    for limit in LIMITS:
        run_indexer(limit)
        stats_path = OUT_DIR / 'stats.txt'
        stats = parse_stats(stats_path)
        stats['Limit'] = limit
        rows.append(stats)

    plot_perf(rows)
    plot_zipf()

    total_docs = mongo_count_articles()
    update_report(total_docs, rows)

    # emit LaTeX table snippet
    print('\nLaTeX table rows:')
    for r in rows:
        print(f"{int(r['Docs'])} & {int(r['Terms'])} & {int(r['Tokens'])} & {r['AvgTokenLen']:.5f} & {r['InputKB']:.0f} & {r['Elapsed']:.5f} & {r['SpeedKBps']:.1f}\\\\")


if __name__ == '__main__':
    main()
