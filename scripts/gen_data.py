"""Generate a text dataset for the example job and the benchmarks.

    python scripts/gen_data.py --out data/bench --size-mb 200 --files 8

Each line is ten words drawn from a fixed vocabulary. Common words are drawn
far more often than rare ones (a Zipf distribution), as in real text. The
output depends only on the arguments, so the same command always produces
the same files.
"""

import argparse
import random
from itertools import accumulate
from pathlib import Path

VOCABULARY = """
the of and to in is that it was for on are as with his they at be this from
have or by one had not but what all were when we there can an your which
their said if do will each about how up out them then she many some so these
would other into has more her two like him see time could no make than first
been its who now people my made over did down only way find use may water
long little very after words called just where most know get through back
much before go good new write our used me man too any day same right look
think also around another came come work three word must because does part
even place well such here take why things help put years different away
again off went old number great tell men say small every found still between
name should home big give air line set own under read last never us left end
along while might next sound below saw something thought both few those
always looked show large often together asked house world going want school
important until form food keep children feet land side without boy once
animals life enough took sometimes four head above kind began almost live
page got earth need far hand high year mother light parts country father let
night following picture being study second eyes soon times story boys since
white days ever paper hard near sentence better best across during today
others however sure means knew try told young miles sun ways thing whole
hear example heard several change answer room sea against top turned learn
point city play toward five using himself usually money seen car morning
given order red door river field stand green yes ocean space wind rock
mountain forest valley bridge garden market village harbor island desert
winter summer spring autumn silver golden copper iron stone glass wooden
quiet gentle bright distant ancient modern simple narrow wide deep shallow
""".split()


def generate(out_dir: Path, size_mb: float, files: int, seed: int) -> None:
    """Write `files` text files into out_dir, about size_mb megabytes in total."""
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    # Word number n is drawn with weight 1/n.
    cumulative_weights = list(accumulate(1 / rank for rank in range(1, len(VOCABULARY) + 1)))
    bytes_per_file = int(size_mb * 1024 * 1024 / files)
    lines_per_batch = 100

    for index in range(files):
        with open(out_dir / f"part-{index:02d}.txt", "w", encoding="ascii") as f:
            written = 0
            while written < bytes_per_file:
                words = rng.choices(VOCABULARY, cum_weights=cumulative_weights, k=10 * lines_per_batch)
                lines = [" ".join(words[start:start + 10]) for start in range(0, len(words), 10)]
                text = "\n".join(lines) + "\n"
                f.write(text)
                written += len(text)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a text dataset.")
    parser.add_argument("--out", required=True, help="directory to write the files into")
    parser.add_argument("--size-mb", type=float, default=1.0, help="total size in megabytes (default: 1)")
    parser.add_argument("--files", type=int, default=4, help="number of files (default: 4)")
    parser.add_argument("--seed", type=int, default=0, help="random seed (default: 0)")
    args = parser.parse_args()

    generate(Path(args.out), args.size_mb, args.files, args.seed)
    print(f"wrote {args.files} files, about {args.size_mb} MB in total, to {args.out}")


if __name__ == "__main__":
    main()
