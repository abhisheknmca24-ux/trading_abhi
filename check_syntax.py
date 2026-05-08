import os
import py_compile
import sys

def check_syntax(directory):
    errors_found = False
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    py_compile.compile(file_path, doraise=True)
                    print(f"OK: {file_path}")
                except py_compile.PyCompileError as e:
                    print(f"ERROR in {file_path}: {e}")
                    errors_found = True
                except Exception as e:
                    print(f"CRITICAL ERROR in {file_path}: {e}")
                    errors_found = True
    return errors_found

if __name__ == "__main__":
    has_errors = check_syntax('.')
    if has_errors:
        sys.exit(1)
    else:
        sys.exit(0)
