from pathlib import Path

SOURCE = Path("../data/instances/MuterOencan")
TARGET = Path("../data/instances/MuterOencanWG")

REPLACEMENTS = {
    "DISTANCE_TOP_TO_CELL : 1": "DISTANCE_TOP_TO_CELL : 1.5",
    "DISTANCE_BOTTOM_TO_CELL : 1": "DISTANCE_BOTTOM_TO_CELL : 1.5",
}


def convert(text: str) -> str:
    for old, new in REPLACEMENTS.items():
        if text.count(old) != 1:
            raise ValueError(f"Expected exactly one occurrence of {old!r}")
        text = text.replace(old, new)
    return text


def main():
    TARGET.mkdir(parents=True, exist_ok=True)

    files = sorted(SOURCE.glob("*.txt"))
    assert len(files) == 270

    for src in files:
        dst = TARGET / src.name
        dst.write_text(convert(src.read_text()), encoding="utf-8")


if __name__ == "__main__":
    main()