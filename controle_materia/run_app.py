import subprocess
import webbrowser
import time
import sys
from pathlib import Path

def main():
    base_dir = Path(__file__).parent

    app_path = base_dir / "app.py"

    # Inicia Streamlit
    subprocess.Popen([
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.headless=true"
    ])

    # Aguarda servidor subir
    time.sleep(3)

    # Abre navegador
    webbrowser.open("http://localhost:8501")


if __name__ == "__main__":
    main()
