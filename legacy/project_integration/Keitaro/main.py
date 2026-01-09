import sys
from pathlib import Path

from Keitaro.services.keitaro_service import KeitaroService


def main() -> None:
    if len(sys.argv) != 2:
        print(
            "Usage:\n"
            "  python -m Keitaro.main <path_to_keitaro_csv>\n\n"
            "Example:\n"
            "  python -m Keitaro.main C:\\Users\\user\\Downloads\\report.csv"
        )
        sys.exit(1)

    csv_path = Path(sys.argv[1])

    if not csv_path.exists():
        print(f"[ERROR] CSV file does not exist: {csv_path}")
        sys.exit(2)

    service = KeitaroService(csv_path)
    service.run()


if __name__ == "__main__":
    main()
