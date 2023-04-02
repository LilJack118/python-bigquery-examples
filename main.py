import sys
import importlib

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide a function name")
        sys.exit(1)

    func = sys.argv[1]
    module = importlib.import_module(f"src.{func}")
    module.main()
