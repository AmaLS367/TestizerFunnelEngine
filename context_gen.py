import os

IGNORE_DIRS = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    ".idea",
    ".vscode",
    ".cursor",
    "node_modules",
    "vendor",
    "cache",
    "logs",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    "dist",
    "build",
    "htmlcov",
    ".hypothesis",
}

IGNORE_DIR_PATTERNS = [
    ".egg-info",
]
INCLUDE_EXT = {
    ".py",
    ".md",
    ".sql",
    ".txt",
    ".yml",
    ".yaml",
    ".json",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
}
IGNORE_FILES = {
    "composer.lock",
    "package-lock.json",
    "yarn.lock",
    "poetry.lock",
    ".gitignore",
    ".gitattributes",
    ".env",
    ".env.example",
    ".DS_Store",
    "Thumbs.db",
    ".coverage",
    "coverage.xml",
    ".pytest_cache",
    "full_project_context_testizer.txt",
}

IGNORE_EXTENSIONS = {
    ".pyc",
    ".pyo",
    ".pyd",
    ".log",
    ".cache",
}


def generate_context():
    output_file = "full_project_context_testizer.txt"

    with open(output_file, "w", encoding="utf-8") as outfile:
        outfile.write("=" * 80 + "\n")
        outfile.write("TESTIZER EMAIL FUNNELS - FULL PROJECT CONTEXT\n")
        outfile.write("=" * 80 + "\n\n")

        for root, dirs, files in os.walk("."):
            dirs[:] = [
                d
                for d in dirs
                if d not in IGNORE_DIRS
                and not any(pattern in d for pattern in IGNORE_DIR_PATTERNS)
            ]

            for file in files:
                if file in IGNORE_FILES:
                    continue

                _, ext = os.path.splitext(file)
                if ext in IGNORE_EXTENSIONS:
                    continue

                if ext in INCLUDE_EXT or file in (
                    "Dockerfile",
                    ".htaccess",
                    "requirements.txt",
                ):
                    path = os.path.join(root, file)

                    path = os.path.normpath(path)

                    outfile.write(f"\n{'='*80}\n")
                    outfile.write(f"FILE: {path}\n")
                    outfile.write(f"{'='*80}\n\n")

                    try:
                        with open(
                            path, "r", encoding="utf-8", errors="ignore"
                        ) as infile:
                            content = infile.read()
                            outfile.write(content)
                            if not content.endswith("\n"):
                                outfile.write("\n")
                    except Exception as e:
                        outfile.write(f"Error reading file: {e}\n")

    print(f"Ready. File {output_file} created.")


if __name__ == "__main__":
    generate_context()
