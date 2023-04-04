import sys
import importlib
import os

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide a function name")
        sys.exit(1)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/service-key.json"
    func = sys.argv[1]
    module = importlib.import_module(f"src.{func}")
    module.main()
