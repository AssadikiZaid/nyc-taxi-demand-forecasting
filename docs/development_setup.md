# Development setup

This guide sets up the project on Windows through WSL2 (Ubuntu). On Linux or macOS, skip
step 1 and install the same tools with your package manager.

## 1. Install WSL2 and Ubuntu (Windows only)

The Linux disk of the distribution grows with the data, the environments and the Docker
images (expect 15 to 20 GB over the project). If the system drive `C:` is short on space,
install the distribution on another drive with `--location`.

In PowerShell **run as administrator**:

```powershell
wsl --install --no-distribution   # installs WSL itself; restart if asked
wsl --update
wsl --install -d Ubuntu-24.04 --location D:\WSL\Ubuntu-24.04
```

Drop `--location ...` to keep the default location on `C:`. When Ubuntu opens, create a
Linux user and password. Check that the distribution runs on WSL 2:

```powershell
wsl -l -v
```

Work inside the Linux file system (for example `~/projects`), not under `/mnt/c/...`:
file access across the Windows boundary is much slower, which matters for Spark.

By default WSL2 gets half of the machine's RAM. To change it, create
`C:\Users\<you>\.wslconfig`:

```ini
[wsl2]
memory=5GB   # about 60 % of the RAM: 5GB on an 8 GB machine, 7GB on a 12 GB machine
```

Then run `wsl --shutdown` and reopen Ubuntu. On an 8 GB machine, the Spark jobs process
one month at a time to stay within this budget.

The project does not need a local GPU: the TFT model is trained on a free Colab or
Kaggle GPU (phase 5).

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
