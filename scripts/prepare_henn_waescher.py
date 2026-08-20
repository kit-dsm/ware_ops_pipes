from pathlib import Path
from shutil import copy2


ROOT = Path(__file__).resolve().parents[1]
INSTANCE_DIR = ROOT / "data" / "instances"
SOURCE = INSTANCE_DIR / "HennWaescher"

SOURCE_DIRS = {
    "HennWaescherUniform": [
        ("Instances_uniform", "Instances_Largest_Gap"),
        ("Instances_uniform", "Instances_S-Shape"),
    ],
    "HennWaescherClassBased": [
        ("Instances_class-based", "Instances_Largest_Gap"),
        ("Instances_class-based", "Instances_S-Shape"),
    ],
}

LOCAL_SOURCE_DIRS = {
    ("Instances_uniform", "Instances_Largest_Gap"): "uniform_Largest_Gap",
    ("Instances_uniform", "Instances_S-Shape"): "uniform_S-Shape",
    ("Instances_class-based", "Instances_Largest_Gap"): "class-based_Largest_Gap",
    ("Instances_class-based", "Instances_S-Shape"): "class-based_S-Shape",
}


def source_path(parts: tuple[str, str]) -> Path:
    published_path = SOURCE.joinpath(*parts)
    if published_path.is_dir():
        return published_path

    local_path = SOURCE / LOCAL_SOURCE_DIRS[parts]
    if local_path.is_dir():
        return local_path

    raise FileNotFoundError(f"Henn--Waescher source directory not found: {published_path}")


def prepare(target_name: str, source_dirs: list[tuple[str, str]]) -> None:
    sources = [source_path(parts) for parts in source_dirs]
    files = [path for directory in sources for path in directory.rglob("*.txt")]

    names = [path.name for path in files]
    if len(files) != 2880 or len(set(names)) != 2880:
        raise ValueError(
            f"{target_name}: expected 2,880 files with unique names, found "
            f"{len(files)} files and {len(set(names))} names"
        )

    target = INSTANCE_DIR / target_name
    target.mkdir(parents=True, exist_ok=True)
    for source in files:
        copy2(source, target / source.name)

    actual = {path.name for path in target.glob("*.txt")}
    expected = set(names)
    if actual != expected:
        raise ValueError(f"{target_name} contains files outside the prepared set")

    print(f"{target_name}: {len(actual)} files")


def main() -> None:
    for target_name, source_dirs in SOURCE_DIRS.items():
        prepare(target_name, source_dirs)


if __name__ == "__main__":
    main()
