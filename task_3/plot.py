import argparse
import csv
import math

import matplotlib.pyplot as plt


def read_csv(path):
    ranks = []
    freqs = []
    zipf = []
    mand = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ranks.append(int(row['rank']))
            freqs.append(float(row['freq']))
            zipf.append(float(row.get('zipf', 0.0) or 0.0))
            mand.append(float(row.get('mandelbrot', 0.0) or 0.0))
    return ranks, freqs, zipf, mand


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--title', default='Zipf Law')
    args = ap.parse_args()

    ranks, freqs, zipf, mand = read_csv(args.input)

    plt.figure(figsize=(8, 6))
    plt.loglog(ranks, freqs, label='Empirical')
    if any(z > 0 for z in zipf):
        plt.loglog(ranks, zipf, label='Zipf')
    if any(m > 0 for m in mand):
        plt.loglog(ranks, mand, label='Mandelbrot')

    plt.xlabel('Rank (log)')
    plt.ylabel('Frequency (log)')
    plt.title(args.title)
    plt.grid(True, which='both', ls='--', alpha=0.4)
    plt.legend()
    plt.tight_layout()
    plt.savefig(args.out, dpi=150)


if __name__ == '__main__':
    main()
