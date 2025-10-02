# SDTmatchASIN

SDTmatchASIN helps catalog teams look up Amazon ASINs starting from European Article Numbers (EANs) by combining Amazon's Selling Partner API (SP-API) catalog endpoints with Product Advertising API (PA-API) lookups and local fuzzy-matching heuristics.

---

## 1. Prerequisites

- Ubuntu 22.04 LTS (or newer) with sudo access
- Python 3.10+ and pip
- Git

> **Tip:** The project ships with a [`requirements.txt`](requirements.txt) file and a sample environment file [`.env.example`](.env.example) to accelerate onboarding.

---

## 2. Ubuntu setup

```bash
# Update base system
sudo apt update && sudo apt upgrade -y

# Install Python build tooling and git
sudo apt install -y python3 python3-pip python3-venv git

# (Optional) Install Docker if you plan to run inside a container
sudo apt install -y docker.io docker-compose-plugin
```

Clone the repository:

```bash
git clone https://github.com/your-org/SDTmatchASIN.git
cd SDTmatchASIN
```

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate

# Upgrade pip and install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 3. Environment configuration

Amazon's APIs require a handful of secrets. Copy the sample file and edit it with your credentials:

```bash
cp .env.example .env
```

| Variable | Description |
| --- | --- |
| `SP_API_REFRESH_TOKEN` | LWA refresh token issued for your Selling Partner API application. |
| `SP_API_CLIENT_ID` / `SP_API_CLIENT_SECRET` | Login With Amazon credentials for your SP-API app. |
| `SP_API_REGION` | API endpoint region (e.g. `eu-west-1`, `us-east-1`). |
| `SP_API_ROLE_ARN` | IAM role ARN granted by the selling partner for SP-API access. |
| `MARKETPLACE_IDS` | Comma-separated list of marketplace IDs to query. |
| `PA_API_ACCESS_KEY` / `PA_API_SECRET_KEY` | AWS access key pair for Product Advertising API. |
| `PA_API_PARTNER_TAG` | Associate tag linked to your PA-API account. |
| `LOG_LEVEL` | Optional runtime log verbosity (`INFO`, `DEBUG`, etc.). |
| `DEFAULT_CURRENCY` | Optional currency hint used during matching. |

Store the `.env` file at the project root. The CLI automatically loads environment variables via `python-dotenv`.

---

## 4. CLI usage

After installing dependencies and populating `.env`, run the CLI to perform an EAN → ASIN lookup. Replace the file paths with your own data.

```bash
# Display CLI help
python -m sdtmatchasin.cli --help

# Match a CSV containing EANs and export enriched results
python -m sdtmatchasin.cli match \
  --ean-file data/eans.csv \
  --output-file output/matches.csv \
  --marketplace-id ATVPDKIKX0DER

# Resume a previous batch with verbose logging
LOG_LEVEL=DEBUG python -m sdtmatchasin.cli match --ean-file data/eans.csv --resume
```

Common commands include:

- `match`: Runs the end-to-end matching pipeline.
- `audit`: Replays a subset of EANs and prints diagnostics for failed matches.
- `cache clear`: Drops any cached responses before a fresh run.

> The CLI uses [`tenacity`](https://tenacity.readthedocs.io/) for resilient retries and [`tqdm`](https://github.com/tqdm/tqdm) for progress visualization. See `python -m sdtmatchasin.cli --help` for the authoritative command list.

---

## 5. Running with Docker

Build the container image and run the CLI without polluting your host environment.

```bash
# Build (rootless, multi-use image)
docker build -t sdtmatchasin:latest .

# Run the CLI using environment variables from the host
docker run --rm \
  --env-file .env \
  -v "$PWD/data:/app/data" \
  -v "$PWD/output:/app/output" \
  sdtmatchasin:latest \
  match --ean-file data/eans.csv --output-file output/matches.csv
```

> The Docker image defines a non-root user (`app`) and sets the entrypoint to `python -m sdtmatchasin.cli`, mirroring the local usage pattern.

---

## 6. Troubleshooting credentials

### SP-API issues

- **`Unauthorized` or `403` errors:** Confirm the IAM role (`SP_API_ROLE_ARN`) is added to the selling partner's SP-API authorization and that the role has attached policies for the Catalog Items API.
- **Refresh token failures:** Refresh tokens are bound to the LWA client. Regenerate the token from Seller Central and verify `SP_API_CLIENT_ID`/`SP_API_CLIENT_SECRET` match the same app.
- **Rate-limit throttling:** The CLI automatically backs off using `tenacity`. If you still hit throttles, reduce batch size or add delays via CLI flags.

### PA-API issues

- **`RequestThrottled` responses:** Ensure your PA-API account has generated the minimum number of qualifying sales in the last 30 days. Otherwise Amazon enforces stricter rate limits.
- **Signature mismatch:** Recreate the access key pair and confirm system clocks are synchronized (`timedatectl status`).
- **Missing locale:** The partner tag must correspond to the marketplace (e.g., `-21` suffix for Germany). Align `PA_API_PARTNER_TAG` with the target region.

### General debugging tips

- Run with `LOG_LEVEL=DEBUG` to surface detailed request/response logs from `loguru`.
- Verify the `.env` file is being loaded (`python -m sdtmatchasin.cli env check`).
- Confirm network egress to `sellingpartnerapi-<region>.amazonaws.com` and `webservices.amazon.<tld>` is allowed from your environment.

---

## 7. Additional resources

- [Selling Partner API documentation](https://developer-docs.amazon.com/sp-api)
- [Product Advertising API documentation](https://webservices.amazon.com/paapi5/documentation/)
- [python-sp-api GitHub](https://github.com/saleweaver/python-amazon-sp-api)

