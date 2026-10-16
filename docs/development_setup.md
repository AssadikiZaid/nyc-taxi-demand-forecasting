# Development setup

This guide sets up the project on Windows through WSL2 (Ubuntu). On Linux or macOS, skip
step 1 and install the same tools with your package manager.

## 1. Install WSL2 and Ubuntu (Windows only)

In PowerShell **run as administrator**:

```powershell
wsl --install -d Ubuntu-24.04
```

Restart when asked, then open "Ubuntu" from the Start menu and create a Linux user.
Check that the distribution runs on WSL 2:

```powershell
wsl -l -v
```

Work inside the Linux file system (for example `~/projects`), not under `/mnt/c/...`:
file access across the Windows boundary is much slower, which matters for Spark.

To give WSL2 more memory (Spark likes it), create `C:\Users\<you>\.wslconfig`:

```ini
[wsl2]
memory=8GB
```

Pick a value around half of your RAM, then run `wsl --shutdown` and reopen Ubuntu.

## 2. System packages and Java

```bash
sudo apt update
sudo apt install -y build-essential git make curl openjdk-17-jdk-headless
java -version   # should print 17.x
```

Spark 4 needs Java 17 or later.

## 3. uv and Python 3.12

[uv](https://docs.astral.sh/uv/) manages both the Python version and the dependencies.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc       # or open a new terminal
uv python install 3.12
```

## 4. Git and GitHub

```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
```

Then authenticate to GitHub, either with an SSH key added to your account or with the
GitHub CLI (`sudo apt install gh` then `gh auth login`).

## 5. Get the project and run the checks

```bash
mkdir -p ~/projects && cd ~/projects
git clone <repository-url>
cd nyc-taxi-demand-forecasting
make doctor
make setup
make test
```

`make setup` creates `.venv/` with the core and development dependencies, and installs
the pre-commit hooks: from then on, every `git commit` runs ruff and a few safety checks
(including one that blocks private keys).

## 6. Editor (optional)

VS Code with the "WSL" extension opens the project inside Ubuntu: run `code .` from the
project folder. Select `.venv/bin/python` as the interpreter.
