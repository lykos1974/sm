# Greek Supermarket Price Finder

A lightweight Python app that:
- collects product prices from Greek supermarkets (currently demo collectors with local data),
- groups items by category,
- finds the best price for a shopping list,
- and matches equivalent items even when product descriptions differ.

## Features

- **Category API**: View all offers grouped by category.
- **Best deals API**: Submit basket items and get the cheapest matching offer per item.
- **Fuzzy description matching**:
  - accent-insensitive,
  - handles Greek/Latin variants,
  - synonym normalization (e.g. `γάλα` / `gala` / `milk`).
- **Web UI** for searching and basket comparison.

## Run from PyCharm (recommended for beginners)

If you are not familiar with coding, yes — **PyCharm is a great way to run this app**.

### 1) Open the project in PyCharm

- Start PyCharm.
- Click **Open**.
- Select the folder that contains this project (`sm`).

### 2) Configure Python interpreter

- Go to **File → Settings → Project → Python Interpreter**.
- Choose an interpreter (Python 3.10+).
- If you do not have one, create a new virtual environment from this screen.

### 3) Create a Run Configuration

- Go to **Run → Edit Configurations...**
- Click **+** and choose **Python**.
- Set:
  - **Name**: `Run Price Finder`
  - **Module name**: `app.main`
  - **Working directory**: your project folder (`sm`)
- Click **Apply** then **OK**.

### 4) Run the app

- Click the green **Run** button (▶).
- You should see this in the Run window:

```text
Server running on http://127.0.0.1:8000
```

### 5) Open the webpage

Open your browser at:

`http://127.0.0.1:8000`

If port 8000 is busy, use terminal run with a different port:

```bash
python -c "from app.main import run; run('127.0.0.1', 8080)"
```

Then open `http://127.0.0.1:8080`.

## Run from terminal (alternative)

### 1) Open a terminal in the project folder

The error in your screenshot means your shell cannot find `workspaces/sm` from your current location.
Use the real path where you downloaded/cloned this project.

Examples:

```bash
# if you cloned it under your home folder
cd ~/sm

# or if it is inside a workspaces folder
cd ~/workspaces/sm
```

If you are on **Windows + WSL**, make sure you run Linux-style paths (`~/...` or `/mnt/c/...`) inside WSL.

### 2) Start the app

```bash
python -m app.main
```

### 3) Open the webpage

Open: `http://127.0.0.1:8000`

## API Examples

```bash
curl 'http://127.0.0.1:8000/api/search?query=tomata'
curl -X POST 'http://127.0.0.1:8000/api/best-deals' \
  -H 'Content-Type: application/json' \
  -d '{"items":["γαλα 1 λιτρο", "τοματα", "ρυζι"]}'
```

## Notes

The data collectors currently read local JSON files to keep the project runnable offline. Replace `app/collectors.py` sources with live supermarket feeds/APIs/scrapers in production.
