"""Run all BLS revisions downloads (CES, QCEW)."""

from pathlib import Path

from bls_revisions import download_ces, download_qcew


def main() -> None:
    data_dir = Path.cwd() / "data"
    print("Downloading CES...")
    download_ces(data_dir=data_dir)
    print("Downloading QCEW...")
    download_qcew(data_dir=data_dir)
    print("Done.")


if __name__ == "__main__":
    main()
