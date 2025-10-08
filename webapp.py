"""Simple Flask UI to run the Amazon EAN matcher interactively."""

from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

from flask import (
    Flask,
    jsonify,
    redirect,
    render_template_string,
    request,
    send_file,
    url_for,
)

from amazon_ean_matcher import normalize_marketplaces, run_matcher

app = Flask(__name__)

UPLOAD_DIR = Path("uploads")
RESULT_DIR = Path("results")
JOB_STATE_DIR = Path("job_state")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
RESULT_DIR.mkdir(parents=True, exist_ok=True)
JOB_STATE_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class JobState:
    job_id: str
    filename: str
    marketplaces: str
    status: str = "queued"
    message: str = "Waiting to start"
    processed: int = 0
    total: int = 0
    output_path: Optional[Path] = None
    error: Optional[str] = None
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def storage_path(self) -> Path:
        return JOB_STATE_DIR / f"{self.job_id}.json"

    def _serializable_dict(self) -> Dict[str, Optional[str]]:
        return {
            "job_id": self.job_id,
            "filename": self.filename,
            "marketplaces": self.marketplaces,
            "status": self.status,
            "message": self.message,
            "processed": self.processed,
            "total": self.total,
            "output_path": str(self.output_path) if self.output_path else None,
            "error": self.error,
        }

    def _save_unlocked(self) -> None:
        payload = json.dumps(self._serializable_dict(), ensure_ascii=False, indent=2)
        storage_path = self.storage_path()
        tmp_path = storage_path.parent / f"{storage_path.name}.tmp"
        tmp_path.write_text(payload, encoding="utf-8")
        tmp_path.replace(storage_path)

    def save(self) -> None:
        with self.lock:
            self._save_unlocked()

    def to_dict(self) -> Dict[str, Optional[str]]:
        with self.lock:
            data = {
                "job_id": self.job_id,
                "filename": self.filename,
                "marketplaces": self.marketplaces,
                "status": self.status,
                "message": self.message,
                "processed": self.processed,
                "total": self.total,
                "download_url": url_for("download", job_id=self.job_id) if self.output_path and self.status == "finished" else None,
                "error": self.error,
            }
        return data

    @classmethod
    def load(cls, job_id: str) -> Optional["JobState"]:
        path = JOB_STATE_DIR / f"{job_id}.json"
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        output_path = Path(data["output_path"]) if data.get("output_path") else None
        return cls(
            job_id=data.get("job_id", job_id),
            filename=data.get("filename", ""),
            marketplaces=data.get("marketplaces", ""),
            status=data.get("status", "queued"),
            message=data.get("message", ""),
            processed=int(data.get("processed", 0) or 0),
            total=int(data.get("total", 0) or 0),
            output_path=output_path,
            error=data.get("error"),
        )


jobs: Dict[str, JobState] = {}


INDEX_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>SDTmatchASIN UI</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 2rem; }
      form { margin-bottom: 2rem; }
      .status { margin-top: 1.5rem; }
      .progress { width: 100%; background: #f1f1f1; border-radius: 4px; overflow: hidden; margin-top: 0.5rem; }
      .progress-bar { height: 1rem; background: #2d6cdf; width: 0%; transition: width 0.3s ease; }
      .hidden { display: none; }
      input[type="text"], input[type="number"] { padding: 0.5rem; width: 100%; max-width: 20rem; }
      input[type="file"] { margin-top: 0.5rem; }
      button { padding: 0.5rem 1rem; background: #2d6cdf; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
      button:disabled { opacity: 0.6; cursor: not-allowed; }
      .message { margin-top: 0.75rem; }
      .error { color: #c62828; }
      .success { color: #2e7d32; }
      label { display: block; margin-top: 1rem; }
    </style>
  </head>
  <body>
    <h1>SDTmatchASIN</h1>
    <p>Upload a CSV containing an <code>ean</code> column, choose target marketplaces, and start the matching process.</p>
    <form id="upload-form" enctype="multipart/form-data" method="post">
      <label>CSV file containing EANs
        <input type="file" name="ean_file" accept=".csv" required>
      </label>
      <label>Marketplaces (comma separated, e.g. <code>de,fr,it</code>)
        <input type="text" name="marketplaces" placeholder="de" required>
      </label>
      <label>Throttle between API requests (seconds)
        <input type="number" name="throttle" value="0.5" step="0.1" min="0" max="5">
      </label>
      <button type="submit">Start matching</button>
    </form>
    <div id="status" class="status hidden">
      <h2>Job status</h2>
      <div id="status-message" class="message"></div>
      <div class="progress">
        <div id="progress-bar" class="progress-bar"></div>
      </div>
      <p><span id="progress-count">0</span> / <span id="progress-total">0</span> EANs processed.</p>
      <p id="download-link" class="hidden"></p>
    </div>
    <script>
      const form = document.getElementById('upload-form');
      const statusContainer = document.getElementById('status');
      const statusMessage = document.getElementById('status-message');
      const progressBar = document.getElementById('progress-bar');
      const progressCount = document.getElementById('progress-count');
      const progressTotal = document.getElementById('progress-total');
      const downloadLink = document.getElementById('download-link');
      let pollTimer = null;

      form.addEventListener('submit', function(event) {
        event.preventDefault();
        const formData = new FormData(form);
        statusContainer.classList.remove('hidden');
        statusMessage.classList.remove('error', 'success');
        statusMessage.textContent = 'Uploading...';
        progressBar.style.width = '0%';
        progressCount.textContent = '0';
        progressTotal.textContent = '0';
        downloadLink.innerHTML = '';
        downloadLink.classList.add('hidden');

        fetch('{{ url_for("start") }}', {
          method: 'POST',
          body: formData
        })
        .then(response => response.json())
        .then(data => {
          if (!data.success) {
            statusMessage.classList.remove('success');
            statusMessage.textContent = data.error || 'Failed to start job.';
            statusMessage.classList.add('error');
            return;
          }
          statusMessage.classList.remove('error');
          statusMessage.textContent = 'Job started. Tracking progress...';
          pollJob(data.job_id);
        })
        .catch(() => {
          statusMessage.classList.remove('success');
          statusMessage.textContent = 'Unable to start job.';
          statusMessage.classList.add('error');
        });
      });

      function pollJob(jobId) {
        if (pollTimer) {
          clearInterval(pollTimer);
        }
        pollTimer = setInterval(() => {
          fetch(`{{ url_for("progress", job_id="__JOB_ID__") }}`.replace('__JOB_ID__', jobId))
            .then(response => response.json())
            .then(data => {
              if (!data.success) {
                statusMessage.textContent = data.error || 'Unknown error.';
                statusMessage.classList.remove('success');
                statusMessage.classList.add('error');
                clearInterval(pollTimer);
                return;
              }
              statusMessage.classList.remove('error', 'success');
              statusMessage.textContent = data.message;
              progressCount.textContent = data.processed;
              progressTotal.textContent = data.total;
              const pct = data.total ? Math.min(100, Math.round((data.processed / data.total) * 100)) : 0;
              progressBar.style.width = pct + '%';
              if (data.status === 'finished' && data.download_url) {
                downloadLink.innerHTML = `<a href="${data.download_url}">Download results</a>`;
                downloadLink.classList.remove('hidden');
                statusMessage.classList.add('success');
                clearInterval(pollTimer);
              } else if (data.status === 'error') {
                statusMessage.classList.remove('success');
                statusMessage.classList.add('error');
                clearInterval(pollTimer);
              }
            })
            .catch(() => {
              statusMessage.classList.remove('success');
              statusMessage.textContent = 'Error fetching progress.';
              statusMessage.classList.add('error');
              clearInterval(pollTimer);
            });
        }, 1500);
      }
    </script>
  </body>
</html>
"""


def _run_job(job: JobState, input_path: Path, output_path: Path, marketplaces: str, throttle: float) -> None:
    normalized = normalize_marketplaces(marketplaces)
    if not normalized:
        with job.lock:
            job.status = "error"
            job.message = "No valid marketplaces provided."
            job.error = job.message
            job._save_unlocked()
        return

    def progress_callback(processed: int, total: int, current_ean: Optional[str]) -> None:
        with job.lock:
            job.processed = processed
            job.total = total
            if job.status != "error":
                if job.status == "queued":
                    job.status = "running"
                if current_ean:
                    job.message = f"Processing {current_ean}"
                else:
                    job.message = "Processing EANs"
            job._save_unlocked()

    try:
        with job.lock:
            job.status = "running"
            job._save_unlocked()
        processed, _ = run_matcher(
            input_path=input_path,
            output_path=output_path,
            marketplaces=normalized,
            throttle_seconds=throttle,
            progress_callback=progress_callback,
        )
        with job.lock:
            job.status = "finished"
            job.message = f"Completed {len(processed)} EANs."
            job.output_path = output_path
            job._save_unlocked()
    except Exception as exc:  # pragma: no cover - runtime safeguard
        with job.lock:
            job.status = "error"
            job.message = "Matching failed."
            job.error = str(exc)
            job._save_unlocked()


@app.route("/", methods=["GET"])
def index():
    return render_template_string(INDEX_TEMPLATE)


@app.route("/start", methods=["POST"])
def start():
    file = (request.files.get("ean_file") or request.files.get("file"))
    marketplaces = request.form.get("marketplaces", "")
    throttle_raw = request.form.get("throttle", "0.5")
    if not file or not file.filename:
        return jsonify({"success": False, "error": "Please upload a CSV file."})

    try:
        throttle = max(0.0, float(throttle_raw))
    except ValueError:
        return jsonify({"success": False, "error": "Invalid throttle value."})

    job_id = uuid.uuid4().hex
    safe_name = Path(file.filename).name
    input_path = UPLOAD_DIR / f"{job_id}_{safe_name}"
    output_path = RESULT_DIR / f"{job_id}_results.csv"
    file.save(input_path)

    job = JobState(job_id=job_id, filename=safe_name, marketplaces=marketplaces, message="Queued")
    jobs[job_id] = job
    job.save()

    thread = threading.Thread(
        target=_run_job,
        args=(job, input_path, output_path, marketplaces, throttle),
        daemon=True,
    )
    thread.start()

    return jsonify({"success": True, "job_id": job_id})


@app.route("/progress/<job_id>")
def progress(job_id: str):
    job = jobs.get(job_id)
    if not job:
        job = JobState.load(job_id)
        if job:
            jobs[job_id] = job
    if not job:
        return jsonify({"success": False, "error": "Job not found."})
    return jsonify({"success": True, **job.to_dict()})


@app.route("/download/<job_id>")
def download(job_id: str):
    job = jobs.get(job_id)
    if not job:
        job = JobState.load(job_id)
        if job:
            jobs[job_id] = job
    if not job or job.status != "finished" or not job.output_path:
        return redirect(url_for("index"))
    return send_file(job.output_path, as_attachment=True, download_name=f"{job.filename.rsplit('.', 1)[0]}_matches.csv")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
