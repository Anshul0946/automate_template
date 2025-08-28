# app.py
import os
import shutil
import json
import base64
import requests
import openpyxl
from PIL import Image
import io
import time
from pathlib import Path
import re
import tempfile
import streamlit as st
from zipfile import ZipFile

# --- Configuration defaults (will be overridden by UI) ---
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
YOUR_SITE_URL = "http://localhost"
YOUR_SITE_NAME = "Advanced Cellular Template Processor"
API_BASE = "https://openrouter.apify.actor/api/v1"
MODEL_SERVICE = "google/gemini-2.5-flash"
MODEL_GENERIC = "google/gemini-2.5-flash-lite"

# --- Schemas (unchanged) ---
SERVICE_SCHEMA = {
    "nr_arfcn": "number", "nr_band": "number", "nr_pci": "number", "nr_bw": "number",
    "nr5g_rsrp": "number", "nr5g_rsrq": "number", "nr5g_sinr": "number",
    "lte_band": "number", "lte_earfcn": "number", "lte_pci": "number",
    "lte_bw": "number", "lte_rsrp": "number", "lte_rsrq": "number", "lte_sinr": "number"
}

GENERIC_SCHEMAS = {
    "speed_test": {
        "image_type": "speed_test",
        "data": {
            "download_mbps": "number",
            "upload_mbps": "number",
            "ping_ms": "number",
            "jitter_ms": "number"
        }
    },
    "video_test": {
        "image_type": "video_test",
        "data": {
            "max_resolution": "string",
            "load_time_ms": "number",
            "buffering_percentage": "number"
        }
    },
    "voice_call": {
        "image_type": "voice_call",
        "data": {
            "phone_number": "string",
            "call_duration_seconds": "number",
            "call_status": "string",
            "time": "string"
        }
    }
}

# --- Global state containers (per-run) ---
# these will be re-initialized inside processing function
alpha_service = {}
beta_service = {}
gamma_service = {}
alpha_speedtest = {}
beta_speedtest = {}
gamma_speedtest = {}
alpha_video = {}
beta_video = {}
gamma_video = {}
voice_test = {}
extract_text = []
avearge = {}

# --- Helpers used by both UI and processing ---
def _apify_headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "HTTP-Referer": YOUR_SITE_URL,
        "X-Title": YOUR_SITE_NAME
    }

def validate_api_key(token: str, timeout: float = 8.0) -> (bool, str):
    """
    Try a lightweight call to check whether the token is authorized.
    Returns (True, '') on success. On failure returns (False, message).
    This uses a tiny model request — it may consume a small amount of quota.
    """
    try:
        test_payload = {
            "model": MODEL_GENERIC,
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "ping"}
                ]}
            ],
            "response_format": {"type": "json_object"}
        }
        resp = requests.post(f"{API_BASE}/chat/completions", headers=_apify_headers(token), data=json.dumps(test_payload), timeout=timeout)
        if resp.status_code == 401:
            return False, "Unauthorized (401) — token invalid or expired."
        if resp.status_code >= 400:
            return False, f"API responded with status {resp.status_code}: {resp.text[:300]}"
        return True, ""
    except requests.exceptions.RequestException as e:
        return False, f"Network error while validating key: {e}"
    except Exception as e:
        return False, f"Validation error: {e}"

def log_append(text_area, logs_list, msg: str):
    logs_list.append(msg)
    # update UI text area
    text_area.text("\n".join(logs_list[-200:]))  # show last 200 lines

# --- Original helpers and analyzers adapted to accept token and not use input() ---
def get_sector_from_col(col_index):
    if 0 <= col_index < 4:
        return "alpha"
    if 4 <= col_index < 8:
        return "beta"
    if 8 <= col_index < 12:
        return "gamma"
    if 12 <= col_index < 18:
        return "voicetest"
    return "unknown"

def extract_images_from_excel(xlsx_path, output_folder, logs, text_area, token):
    log_append(text_area, logs, f"[LOG] Analyzing template file: {xlsx_path}")
    try:
        workbook = openpyxl.load_workbook(xlsx_path)
        sheet = workbook.active
    except Exception as e:
        log_append(text_area, logs, f"[ERROR] Could not open or read the Excel file. {e}")
        return []

    images_with_locations = []
    if not sheet._images:
        log_append(text_area, logs, "[WARN] No images found in the Excel sheet.")
        return []

    for image in sheet._images:
        row = image.anchor._from.row + 1
        col = image.anchor._from.col
        images_with_locations.append({"image": image, "row": row, "col": col})

    sorted_images = sorted(images_with_locations, key=lambda i: (i['row'], i['col']))

    saved_image_paths = []
    sector_counters = {"alpha": 0, "beta": 0, "gamma": 0, "voicetest": 0, "unknown": 0}

    log_append(text_area, logs, f"[LOG] Found {len(sorted_images)} images. Extracting and naming them...")
    for item in sorted_images:
        sector = get_sector_from_col(item['col'])
        sector_counters[sector] += 1

        filename = f"{sector}_image_{sector_counters[sector]}.png"
        output_path = os.path.join(output_folder, filename)

        try:
            img_data = item['image']._data()
            pil_img = Image.open(io.BytesIO(img_data))
            pil_img.save(output_path, 'PNG')
            saved_image_paths.append(output_path)
            log_append(text_area, logs, f"  - Saved '{filename}' (from cell approx. {openpyxl.utils.get_column_letter(item['col']+1)}{item['row']})")
        except Exception as e:
            log_append(text_area, logs, f"[ERROR] Failed to save image {filename}. Error: {e}")

    return saved_image_paths

def get_images_from_uploaded_files(uploaded_files, temp_dir, logs, text_area):
    log_append(text_area, logs, f"[LOG] Received {len(uploaded_files)} uploaded file(s) for images.")
    copied_paths = []
    for f in uploaded_files:
        try:
            dest_path = os.path.join(temp_dir, f.name)
            with open(dest_path, "wb") as out:
                out.write(f.getbuffer())
            copied_paths.append(dest_path)
        except Exception as e:
            log_append(text_area, logs, f"[ERROR] Failed to save uploaded image {f.name}: {e}")
    log_append(text_area, logs, f"[LOG] Saved {len(copied_paths)} images to temp dir.")
    return copied_paths

def _normalize_name(n: str) -> str:
    return re.sub(r'[^0-9a-zA-Z]', '', n).lower()

key_pattern = re.compile(r"\[['\"]([^'\"]+)['\"]\]")

def resolve_expression_with_vars(expr: str, allowed_vars: dict):
    expr = expr.strip()
    m = re.match(r"^([A-Za-z_]\w*)(.*)$", expr)
    if not m:
        return None
    base_raw = m.group(1)
    rest = m.group(2) or ""

    norm_map = {_normalize_name(k): k for k in allowed_vars.keys()}
    base_norm = _normalize_name(base_raw)
    base_key = norm_map.get(base_norm)
    if not base_key:
        for k in allowed_vars.keys():
            if k.lower() == base_raw.lower():
                base_key = k
                break
    if not base_key:
        return None

    obj = allowed_vars[base_key]
    if rest.strip() == "":
        return obj

    keys = key_pattern.findall(rest)
    if not keys:
        return None

    try:
        for k in keys:
            if not isinstance(obj, dict):
                return None
            if k in obj:
                obj = obj[k]
                continue
            found = None
            for real_k in obj.keys():
                if real_k.lower() == k.lower() or _normalize_name(real_k) == _normalize_name(k):
                    found = real_k
                    break
            if found:
                obj = obj[found]
            else:
                return None
        return obj
    except Exception:
        return None

def set_nested_value_case_insensitive(target_dict: dict, keys: list, value):
    cur = target_dict
    for i, k in enumerate(keys):
        if i == len(keys) - 1:
            if isinstance(cur, dict):
                found = None
                for real_k in cur.keys():
                    if real_k.lower() == k.lower() or _normalize_name(real_k) == _normalize_name(k):
                        found = real_k
                        break
                if found:
                    cur[found] = value
                else:
                    cur[k] = value
            return True
        else:
            found = None
            if isinstance(cur, dict):
                for real_k in cur.keys():
                    if real_k.lower() == k.lower() or _normalize_name(real_k) == _normalize_name(k):
                        found = real_k
                        break
            if found:
                if not isinstance(cur[found], dict):
                    cur[found] = {}
                cur = cur[found]
            else:
                cur[k] = {}
                cur = cur[k]
    return True

def ask_model_for_expression_value(token: str, var_name: str, var_obj, expression: str, model_name: str):
    try:
        var_json = json.dumps(var_obj, indent=2)
    except Exception:
        var_json = json.dumps(str(var_obj))

    prompt = f"""
You are an exacting assistant. You are given a JSON variable named "{var_name}" with the following content:

{var_json}

Given the expression:
{expression}

Using only the provided JSON variable (do not assume other data), evaluate the expression and return a single JSON object:
{{ "value": <value> }}

Where <value> is the exact value (number, string) if present, or null if the expression cannot be evaluated from the JSON. Return only the JSON object and nothing else.
"""

    payload = {
        "model": model_name,
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
        "response_format": {"type": "json_object"}
    }

    try:
        response = requests.post(url=f"{API_BASE}/chat/completions", headers=_apify_headers(token), data=json.dumps(payload), timeout=30)
        response.raise_for_status()
        content = response.json()['choices'][0]['message']['content']
        parsed = json.loads(content)
        return parsed.get("value", None)
    except Exception as e:
        return None

# --- API image analyzers (same prompts) ---
def process_service_images(token: str, image1_path, image2_path, model_name, logs, text_area):
    sector = Path(image1_path).stem.split('_')[0]
    log_append(text_area, logs, f"[LOG] Starting specialized service data extraction for '{sector}' sector.")
    log_append(text_area, logs, f"[LOG] Using model: {model_name}")
    try:
        with open(image1_path, "rb") as f:
            b64_img1 = base64.b64encode(f.read()).decode('utf-8')
        with open(image2_path, "rb") as f:
            b64_img2 = base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        log_append(text_area, logs, f"[ERROR] Could not read or encode service images: {e}")
        return None

    prompt = f"""
You are a hyper-specialized AI for cellular network engineering data analysis. Your task is to analyze the two provided service mode screenshots with extreme precision. These images contain high-stakes technical data.

Instructions:
1.  Examine BOTH images carefully. Some parameters might be in the first image, and others in the second. Synthesize the data from both to create one complete report.
2.  Extract only the values for the parameters listed in the schema below.
3.  Provide your output *only* as a single, valid JSON object that strictly follows this schema. Do not include any extra text or explanations. If a parameter is not found in either image, use JSON 'null'.

SCHEMA:
{json.dumps(SERVICE_SCHEMA, indent=2)}
"""

    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_img1}"}},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_img2}"}}
                ]
            }
        ],
        "response_format": {"type": "json_object"}
    }

    try:
        response = requests.post(url=f"{API_BASE}/chat/completions", headers=_apify_headers(token), data=json.dumps(payload), timeout=120)
        response.raise_for_status()
        result = json.loads(response.json()['choices'][0]['message']['content'])
        log_append(text_area, logs, f"[SUCCESS] AI successfully processed service data for '{sector}'.")
        return result
    except Exception as e:
        log_append(text_area, logs, f"[ERROR] API call failed for service images. Error: {e}")
        if 'response' in locals():
            log_append(text_area, logs, f"  Response: {response.text}")
        return None
    finally:
        log_append(text_area, logs, "[LOG] Cooldown: Waiting for 2 seconds...")
        time.sleep(2)

def analyze_generic_image(token: str, image_path, model_name, logs, text_area):
    image_name = Path(image_path).name
    log_append(text_area, logs, f"[LOG] Starting generic data extraction for '{image_name}'.")
    log_append(text_area, logs, f"[LOG] Using model: {model_name}")
    try:
        with open(image_path, "rb") as f:
            b64_img = base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        log_append(text_area, logs, f"[ERROR] Could not read or encode image '{image_name}': {e}")
        return None

    prompt = f"""
You are an expert AI assistant for analyzing cellular network test data. Your task is to:
1.  Classify the image as 'speed_test', 'video_test', or 'voice_call'.
2.  Extract all key values with high accuracy.
3.  Provide your output *only* as a single, valid JSON object that strictly adheres to the corresponding schema below. Do not add any extra text. Use JSON 'null' for missing values.

SCHEMAS:
{json.dumps(GENERIC_SCHEMAS, indent=2)}
"""

    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_img}"}}
                ]
            }
        ],
        "response_format": {"type": "json_object"}
    }

    try:
        response = requests.post(url=f"{API_BASE}/chat/completions", headers=_apify_headers(token), data=json.dumps(payload), timeout=60)
        response.raise_for_status()
        result = json.loads(response.json()['choices'][0]['message']['content'])
        log_append(text_area, logs, f"[SUCCESS] AI successfully processed '{image_name}' as type '{result.get('image_type', 'unknown')}'.")
        return result
    except Exception as e:
        log_append(text_area, logs, f"[ERROR] API call failed for '{image_name}'. Error: {e}")
        if 'response' in locals():
            log_append(text_area, logs, f"  Response: {response.text}")
        return None
    finally:
        log_append(text_area, logs, "[LOG] Cooldown: Waiting for 2 seconds...")
        time.sleep(2)

# evaluate and careful eval wrappers
def evaluate_service_images(token: str, image1_path, image2_path, model_name, logs, text_area):
    sector = Path(image1_path).stem.split('_')[0]
    log_append(text_area, logs, f"[EVAL] Re-evaluating service images for '{sector}' (careful mode).")
    try:
        with open(image1_path, "rb") as f:
            b64_img1 = base64.b64encode(f.read()).decode('utf-8')
        with open(image2_path, "rb") as f:
            b64_img2 = base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        log_append(text_area, logs, f"[EVAL ERROR] Could not read or encode service images: {e}")
        return None

    prompt = f"""
You are an expert cellular network data extraction system. THIS IS A CAREFUL, LINE-BY-LINE EVALUATION.
- Read both images slowly and carefully.
- For each field in the schema, examine the images line-by-line and return the exact value.
- Use the SCHEMA below. Return exactly ONE JSON object matching the schema. No explanations.
- If you are absolutely certain a value is not present, return null for that field. Otherwise, return the value.
- Do not add or remove schema fields. Use numbers where expected.

SCHEMA:
{json.dumps(SERVICE_SCHEMA, indent=2)}
"""

    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_img1}"}},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_img2}"}}
                ]
            }
        ],
        "response_format": {"type": "json_object"}
    }

    try:
        response = requests.post(url=f"{API_BASE}/chat/completions", headers=_apify_headers(token), data=json.dumps(payload), timeout=120)
        response.raise_for_status()
        return json.loads(response.json()['choices'][0]['message']['content'])
    except Exception as e:
        log_append(text_area, logs, f"[EVAL ERROR] Service evaluation API failed: {e}")
        if 'response' in locals():
            log_append(text_area, logs, f"  Response: {response.text}")
        return None
    finally:
        log_append(text_area, logs, "[EVAL] Cooldown: Waiting for 2 seconds...")
        time.sleep(2)

def evaluate_generic_image(token: str, image_path, model_name, logs, text_area):
    image_name = Path(image_path).name
    log_append(text_area, logs, f"[EVAL] Re-evaluating image '{image_name}' (careful mode).")
    try:
        with open(image_path, "rb") as f:
            b64_img = base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        log_append(text_area, logs, f"[EVAL ERROR] Could not read or encode image '{image_name}': {e}")
        return None

    prompt = f"""
You are an expert cellular network data extraction system. THIS IS A CAREFUL, LINE-BY-LINE EVALUATION.
- Analyze this image slowly and carefully.
- Classify it as 'speed_test', 'video_test', or 'voice_call'.
- For the detected type, extract all schema fields, line-by-line, with high confidence.
- Return a single JSON object ONLY and EXACTLY matching one of the schemas below. If a field truly cannot be found, use null.
- Do not add extraneous text.

SCHEMAS:
{json.dumps(GENERIC_SCHEMAS, indent=2)}
"""

    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64_img}"}}
                ]
            }
        ],
        "response_format": {"type": "json_object"}
    }

    try:
        response = requests.post(url=f"{API_BASE}/chat/completions", headers=_apify_headers(token), data=json.dumps(payload), timeout=90)
        response.raise_for_status()
        return json.loads(response.json()['choices'][0]['message']['content'])
    except Exception as e:
        log_append(text_area, logs, f"[EVAL ERROR] Generic evaluation API failed for '{image_name}': {e}")
        if 'response' in locals():
            log_append(text_area, logs, f"  Response: {response.text}")
        return None
    finally:
        log_append(text_area, logs, "[EVAL] Cooldown: Waiting for 2 seconds...")
        time.sleep(2)

# -------------------- MAIN PROCESSING function (adapted for Streamlit) --------------------
def process_file_streamlit(user_path: str, token: str, temp_dir: str, image_uploads=None, logs=None, text_area=None):
    """
    Runs the full processing pipeline on the provided file path.
    image_uploads: list of uploaded image file objects (for CSV workflows)
    logs: list to collect log lines
    text_area: streamlit placeholder that displays logs
    """
    # use module-level globals but reset them for each run
    global alpha_service, beta_service, gamma_service
    global alpha_speedtest, beta_speedtest, gamma_speedtest
    global alpha_video, beta_video, gamma_video
    global voice_test, extract_text, avearge

    alpha_service = {}
    beta_service = {}
    gamma_service = {}
    alpha_speedtest = {}
    beta_speedtest = {}
    gamma_speedtest = {}
    alpha_video = {}
    beta_video = {}
    gamma_video = {}
    voice_test = {}
    extract_text = []
    avearge = {}

    # Validate token quick check before expensive work (we still rely on later API calls)
    ok, msg = validate_api_key(token)
    if not ok:
        log_append(text_area, logs, f"[ERROR] API key validation failed: {msg}")
        return None

    path_obj = Path(user_path)
    image_paths = []

    try:
        if path_obj.is_file() and path_obj.suffix.lower() == '.xlsx':
            log_append(text_area, logs, "[LOG] XLSX template detected. Extracting images automatically.")
            image_paths = extract_images_from_excel(str(path_obj), temp_dir, logs, text_area, token)
        elif path_obj.is_file() and path_obj.suffix.lower() == '.csv':
            log_append(text_area, logs, "[LOG] CSV template detected. Using uploaded images.")
            if not image_uploads:
                log_append(text_area, logs, "[ERROR] No images uploaded for CSV processing.")
                return None
            image_paths = get_images_from_uploaded_files(image_uploads, temp_dir, logs, text_area)
        else:
            log_append(text_area, logs, "[ERROR] Invalid path. Provide .xlsx or .csv.")
            return None

        if not image_paths:
            log_append(text_area, logs, "[ERROR] No images found or extracted. Exiting.")
            return None

        # Group images by sector
        images_by_sector = {"alpha": [], "beta": [], "gamma": [], "voicetest": [], "unknown": []}
        for p in image_paths:
            sector = Path(p).stem.split('_')[0]
            if sector in images_by_sector:
                images_by_sector[sector].append(p)
            else:
                images_by_sector["unknown"].append(p)

        # Main processing loop (unmodified logic but using token + UI logging)
        for sector in ["alpha", "beta", "gamma"]:
            log_append(text_area, logs, f"\n--- Processing Sector: {sector.upper()} ---")
            sector_images = images_by_sector[sector]

            img1 = next((p for p in sector_images if Path(p).stem.endswith("_image_1")), None)
            img2 = next((p for p in sector_images if Path(p).stem.endswith("_image_2")), None)

            if img1 and img2:
                service_data = process_service_images(token, img1, img2, MODEL_SERVICE, logs, text_area)
                if service_data:
                    if sector == "alpha":
                        alpha_service = service_data
                    elif sector == "beta":
                        beta_service = service_data
                    elif sector == "gamma":
                        gamma_service = service_data
            else:
                log_append(text_area, logs, f"[WARN] Could not find both image_1 and image_2 for sector '{sector}'.")

            other_images = [p for p in sector_images if not (Path(p).stem.endswith("_image_1") or Path(p).stem.endswith("_image_2"))]
            for img_path in other_images:
                result = analyze_generic_image(token, img_path, MODEL_GENERIC, logs, text_area)
                if result and 'image_type' in result:
                    image_name = Path(img_path).stem
                    if result['image_type'] == 'speed_test':
                        if sector == "alpha":
                            alpha_speedtest[image_name] = result['data']
                        elif sector == "beta":
                            beta_speedtest[image_name] = result['data']
                        elif sector == "gamma":
                            gamma_speedtest[image_name] = result['data']
                    elif result['image_type'] == 'video_test':
                        if sector == "alpha":
                            alpha_video[image_name] = result['data']
                        elif sector == "beta":
                            beta_video[image_name] = result['data']
                        elif sector == "gamma":
                            gamma_video[image_name] = result['data']
                    elif result['image_type'] == 'voice_call':
                        voice_test[image_name] = result['data']

        # Voice-test sector
        if images_by_sector["voicetest"]:
            log_append(text_area, logs, "\n--- Processing Sector: VOICETEST ---")
            for img_path in images_by_sector["voicetest"]:
                result = analyze_voice_image(img_path, MODEL_GENERIC, logs, text_area)
                if result and result.get('image_type') == 'voice_call':
                    image_name = Path(img_path).stem
                    voice_test[image_name] = result['data']

        # --- EVALUATION pass & RULE2 (keeps earlier behavior) ---
        log_append(text_area, logs, "\n[LOG] Starting evaluation pass to refill missing/null fields (one retry per item).")
        retried_service_sectors = set()
        retried_images = set()

        def contains_nulls(d):
            if not isinstance(d, dict):
                return False
            for v in d.values():
                if v is None:
                    return True
                if isinstance(v, dict) and contains_nulls(v):
                    return True
            return False

        for sector in ["alpha", "beta", "gamma"]:
            svc_var = {"alpha": alpha_service, "beta": beta_service, "gamma": gamma_service}[sector]
            if not svc_var:
                img1 = next((p for p in images_by_sector[sector] if Path(p).stem.endswith("_image_1")), None)
                img2 = next((p for p in images_by_sector[sector] if Path(p).stem.endswith("_image_2")), None)
                if img1 and img2 and sector not in retried_service_sectors:
                    log_append(text_area, logs, f"[EVAL] Service dict for '{sector}' is empty. Re-evaluating service images.")
                    eval_res = evaluate_service_images(token, img1, img2, MODEL_SERVICE, logs, text_area)
                    retried_service_sectors.add(sector)
                    if eval_res:
                        if sector == "alpha":
                            alpha_service = eval_res
                        if sector == "beta":
                            beta_service = eval_res
                        if sector == "gamma":
                            gamma_service = eval_res
                continue

            if contains_nulls(svc_var) and sector not in retried_service_sectors:
                img1 = next((p for p in images_by_sector[sector] if Path(p).stem.endswith("_image_1")), None)
                img2 = next((p for p in images_by_sector[sector] if Path(p).stem.endswith("_image_2")), None)
                if img1 and img2:
                    log_append(text_area, logs, f"[EVAL] Found nulls in {sector}_service; re-evaluating service images for sector '{sector}'.")
                    eval_res = evaluate_service_images(token, img1, img2, MODEL_SERVICE, logs, text_area)
                    retried_service_sectors.add(sector)
                    if eval_res:
                        target = {"alpha": alpha_service, "beta": beta_service, "gamma": gamma_service}[sector]
                        for k, v in eval_res.items():
                            if (target.get(k) is None) and v is not None:
                                target[k] = v

        # helper to retry single images (unchanged)
        def _retry_image_and_merge(image_name, sector_var_map):
            image_path = os.path.join(temp_dir, f"{image_name}.png")
            if not os.path.exists(image_path):
                found = None
                for s_list in images_by_sector.values():
                    for p in s_list:
                        if Path(p).stem == image_name:
                            found = p
                            break
                    if found:
                        break
                if found:
                    image_path = found
                else:
                    log_append(text_area, logs, f"[EVAL WARN] Image file not found for {image_name}. Skipping.")
                    return False
            if image_path in retried_images:
                return False

            is_voice = image_name.startswith("voicetest")
            log_append(text_area, logs, f"[EVAL] Attempting normal analyze for {image_name}.")
            if is_voice:
                normal_res = analyze_voice_image(image_path, MODEL_GENERIC, logs, text_area)
            else:
                normal_res = analyze_generic_image(token, image_path, MODEL_GENERIC, logs, text_area)

            retried_images.add(image_path)
            if normal_res and 'image_type' in normal_res:
                sector_var_map.setdefault(image_name, {})
                data = normal_res.get('data', {})
                for k, v in data.items():
                    if sector_var_map[image_name].get(k) is None and v is not None:
                        sector_var_map[image_name][k] = v
                return True

            log_append(text_area, logs, f"[EVAL] Normal analyze did not yield usable data for {image_name}. Trying careful evaluation.")
            if is_voice:
                eval_res = evaluate_voice_image(image_path, MODEL_GENERIC, logs, text_area)
            else:
                eval_res = evaluate_generic_image(token, image_path, MODEL_GENERIC, logs, text_area)

            if not eval_res or 'image_type' not in eval_res:
                log_append(text_area, logs, f"[EVAL] Careful evaluation returned nothing usable for {image_name}.")
                return False

            sector_var_map.setdefault(image_name, {})
            for k, v in eval_res.get('data', {}).items():
                if sector_var_map[image_name].get(k) is None and v is not None:
                    sector_var_map[image_name][k] = v
            return True

        sector_maps = [
            ("alpha", alpha_speedtest, alpha_video),
            ("beta", beta_speedtest, beta_video),
            ("gamma", gamma_speedtest, gamma_video),
        ]

        expected_indices = {
            "service": [1, 2],
            "speed": [3, 4, 5, 6, 7],
            "video": [8]
        }

        def missing_service_fields(svc_obj):
            missing = []
            for k in SERVICE_SCHEMA.keys():
                if k not in svc_obj or svc_obj.get(k) is None:
                    missing.append(k)
            return missing

        for sector, speed_map, video_map in sector_maps:
            log_append(text_area, logs, f"[RULE2] Verifying expected images & schema completeness for sector '{sector}'.")
            svc_var = {"alpha": alpha_service, "beta": beta_service, "gamma": gamma_service}[sector]
            svc_missing = missing_service_fields(svc_var) if svc_var else list(SERVICE_SCHEMA.keys())
            if svc_missing:
                log_append(text_area, logs, f"[RULE2] Service for '{sector}' missing fields: {svc_missing}")
                img1 = next((p for p in images_by_sector[sector] if Path(p).stem.endswith("_image_1")), None)
                img2 = next((p for p in images_by_sector[sector] if Path(p).stem.endswith("_image_2")), None)
                if img1 and img2 and sector not in retried_service_sectors:
                    log_append(text_area, logs, f"[RULE2] Attempting re-process of service images for '{sector}'.")
                    normal_svc = process_service_images(token, img1, img2, MODEL_SERVICE, logs, text_area)
                    retried_service_sectors.add(sector)
                    if normal_svc:
                        target = {"alpha": alpha_service, "beta": beta_service, "gamma": gamma_service}[sector]
                        for k, v in normal_svc.items():
                            if target.get(k) is None and v is not None:
                                target[k] = v
                        if missing_service_fields(target):
                            log_append(text_area, logs, f"[RULE2] Attempting careful evaluation of service images for '{sector}'.")
                            eval_svc = evaluate_service_images(token, img1, img2, MODEL_SERVICE, logs, text_area)
                            if eval_svc:
                                for k, v in eval_svc.items():
                                    if target.get(k) is None and v is not None:
                                        target[k] = v
                else:
                    log_append(text_area, logs, f"[RULE2] Cannot re-process service for '{sector}': service images missing or already retried.")

            for idx in expected_indices['speed']:
                name = f"{sector}_image_{idx}"
                if name not in speed_map:
                    log_append(text_area, logs, f"[RULE2] Missing expected speed image {name}. Looking for the file to process.")
                    file_path = next((p for p in images_by_sector[sector] if Path(p).stem == name), None)
                    if file_path:
                        log_append(text_area, logs, f"[RULE2] Found file for {name}. Processing.")
                        _retry_image_and_merge(name, speed_map)
                    else:
                        log_append(text_area, logs, f"[RULE2] No file found for expected image {name}.")
                else:
                    missing = []
                    for k in GENERIC_SCHEMAS['speed_test']['data'].keys():
                        if k not in speed_map[name] or speed_map[name].get(k) is None:
                            missing.append(k)
                    if missing:
                        log_append(text_area, logs, f"[RULE2] {name} has missing speed fields {missing}. Re-evaluating the image.")
                        _retry_image_and_merge(name, speed_map)

            for idx in expected_indices['video']:
                name = f"{sector}_image_{idx}"
                if name not in video_map:
                    file_path = next((p for p in images_by_sector[sector] if Path(p).stem == name), None)
                    if file_path:
                        log_append(text_area, logs, f"[RULE2] {sector}_video missing entry {name}. Processing file.")
                        _retry_image_and_merge(name, video_map)
                    else:
                        log_append(text_area, logs, f"[RULE2] No file found for expected video {name}.")
                else:
                    missing = []
                    for k in GENERIC_SCHEMAS['video_test']['data'].keys():
                        if k not in video_map[name] or video_map[name].get(k) is None:
                            missing.append(k)
                    if missing:
                        log_append(text_area, logs, f"[RULE2] {name} video has missing fields {missing}. Re-evaluating the image.")
                        _retry_image_and_merge(name, video_map)

        log_append(text_area, logs, "[RULE2] Verifying voicetest images and schema completeness.")
        for idx in [1, 2, 3]:
            name = f"voicetest_image_{idx}"
            if name not in voice_test:
                file_path = next((p for p in images_by_sector["voicetest"] if Path(p).stem == name), None)
                if file_path:
                    log_append(text_area, logs, f"[RULE2] Missing voice_test entry {name}. Processing file.")
                    _retry_image_and_merge(name, voice_test)
                else:
                    log_append(text_area, logs, f"[RULE2] No file found for expected voice test image {name}.")
            else:
                missing = []
                for k in GENERIC_SCHEMAS['voice_call']['data'].keys():
                    if k not in voice_test[name] or voice_test[name].get(k) is None:
                        missing.append(k)
                if missing:
                    log_append(text_area, logs, f"[RULE2] {name} has missing voice fields {missing}. Re-evaluating the image.")
                    _retry_image_and_merge(name, voice_test)

        log_append(text_area, logs, "[LOG] Rule 2 verification complete.\n")
        log_append(text_area, logs, "[LOG] Evaluation pass complete.\n")

        # ---------- compute avearge ----------
        def _to_number(v):
            try:
                if v is None:
                    return None
                if isinstance(v, bool):
                    return None
                return float(v)
            except Exception:
                return None

        def _compute_speed_averages(speed_map):
            metrics = {"download_mbps": [], "upload_mbps": [], "ping_ms": []}
            for entry in speed_map.values():
                if not isinstance(entry, dict):
                    continue
                for m in metrics.keys():
                    val = _to_number(entry.get(m))
                    if val is not None:
                        metrics[m].append(val)
            result = {}
            for m, vals in metrics.items():
                if vals:
                    result[m] = sum(vals) / len(vals)
                else:
                    result[m] = None
            return result

        avearge = {
            "avearge_alpha_speedtest": _compute_speed_averages(alpha_speedtest),
            "avearge_beta_speedtest": _compute_speed_averages(beta_speedtest),
            "avearge_gamma_speedtest": _compute_speed_averages(gamma_speedtest),
        }

        # -------------------- INITIAL MAPPING: extract bold+red expressions and replace (STRICT) --------------------
        log_append(text_area, logs, "[LOG] Scanning workbook for BOLD + RED expressions (columns A..P ONLY) and replacing them with values.")
        try:
            wb_edit = openpyxl.load_workbook(user_path)
            sheet_edit = wb_edit.active

            cells_to_process = []

            def _font_is_strict_red(font):
                if not font:
                    return False
                if not getattr(font, "bold", False):
                    return False
                col = getattr(font, "color", None)
                if col is None:
                    return False
                rgb = getattr(col, "rgb", None)
                if not rgb:
                    return False
                up = str(rgb).upper()
                last6 = up[-6:]
                return last6 == "FF0000"

            def _normalize_expr(raw):
                s = raw.strip()
                if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
                    s = s[1:-1].strip()
                return s

            for row in sheet_edit.iter_rows(min_row=1, max_row=sheet_edit.max_row, min_col=1, max_col=16):
                for cell in row:
                    val = cell.value
                    if not val or not isinstance(val, str):
                        continue
                    font = cell.font
                    if not font:
                        continue
                    if _font_is_strict_red(font):
                        expr = _normalize_expr(val)
                        if expr:
                            extract_text.append(expr)
                            cells_to_process.append((cell, expr))

            allowed_vars = {
                "alpha_service": alpha_service,
                "beta_service": beta_service,
                "gamma_service": gamma_service,
                "alpha_speedtest": alpha_speedtest,
                "beta_speedtest": beta_speedtest,
                "gamma_speedtest": gamma_speedtest,
                "alpha_video": alpha_video,
                "beta_video": beta_video,
                "gamma_video": gamma_video,
                "voice_test": voice_test,
                "avearge": avearge,
            }

            def _to_number_convert(v):
                try:
                    if v is None:
                        return None
                    if isinstance(v, (int, float)):
                        return v
                    if isinstance(v, bool):
                        return None
                    s = str(v).strip()
                    s_clean = s.replace(',', '')
                    if s_clean == "":
                        return None
                    if re.fullmatch(r"[-+]?\d+", s_clean):
                        return int(s_clean)
                    if re.fullmatch(r"[-+]?\d*\.\d+", s_clean):
                        return float(s_clean)
                    return None
                except Exception:
                    return None

            # initial replacements
            for cell_obj, expr in cells_to_process:
                resolved = resolve_expression_with_vars(expr, allowed_vars)
                if resolved is None:
                    cell_obj.value = "NULL"
                else:
                    if isinstance(resolved, str):
                        num = _to_number_convert(resolved)
                        if num is not None:
                            cell_obj.value = num
                        else:
                            cell_obj.value = resolved
                    elif isinstance(resolved, (int, float)):
                        cell_obj.value = resolved
                    elif isinstance(resolved, (dict, list)):
                        try:
                            cell_obj.value = json.dumps(resolved)
                        except Exception:
                            cell_obj.value = str(resolved)
                    else:
                        cell_obj.value = str(resolved)

            wb_edit.save(user_path)
            log_append(text_area, logs, f"[LOG] Workbook updated and saved to: {user_path}")
        except Exception as e:
            log_append(text_area, logs, f"[ERROR] Failed to edit/save workbook with extracted text replacements: {e}")

        # -------------------- RULE 3: If any bold+red cells are still NULL, do careful remap using AI --------------------
        log_append(text_area, logs, "[LOG] Running Rule 3: remap any bold+red expressions that remained NULL using careful AI re-checks.")
        try:
            wb_r3 = openpyxl.load_workbook(user_path)
            sheet_r3 = wb_r3.active

            # re-bind allowed_vars
            allowed_vars = {
                "alpha_service": alpha_service,
                "beta_service": beta_service,
                "gamma_service": gamma_service,
                "alpha_speedtest": alpha_speedtest,
                "beta_speedtest": beta_speedtest,
                "gamma_speedtest": gamma_speedtest,
                "alpha_video": alpha_video,
                "beta_video": beta_video,
                "gamma_video": gamma_video,
                "voice_test": voice_test,
                "avearge": avearge,
            }

            # collect cells that are still "NULL" (strict bold+red)
            problematic_cells = []
            for row in sheet_r3.iter_rows(min_row=1, max_row=sheet_r3.max_row, min_col=1, max_col=16):
                for cell in row:
                    val = cell.value
                    if not isinstance(val, str):
                        continue
                    if val.strip().upper() != "NULL":
                        continue
                    font = cell.font
                    if font and _font_is_strict_red(font):
                        problematic_cells.append(cell)

            remapped_count = 0
            for cell in problematic_cells:
                candidate = None
                for expr in extract_text:
                    mm = re.match(r"^([A-Za-z_]\w*)", expr.strip())
                    if not mm:
                        continue
                    base_raw = mm.group(1)
                    if _normalize_name(base_raw) in {_normalize_name(k) for k in allowed_vars.keys()}:
                        if resolve_expression_with_vars(expr, allowed_vars) is None:
                            candidate = expr
                            break
                if not candidate:
                    continue

                expr = candidate
                log_append(text_area, logs, f"[RULE3] Attempting strict re-map for expression '{expr}' for cell {cell.coordinate}.")

                m = re.match(r"^([A-Za-z_]\w*)(.*)$", expr)
                if not m:
                    log_append(text_area, logs, f"[RULE3] Could not parse expression '{expr}'. Skipping.")
                    continue
                base_raw = m.group(1)
                rest = m.group(2) or ""
                norm_map = {_normalize_name(k): k for k in allowed_vars.keys()}
                base_key = norm_map.get(_normalize_name(base_raw))
                if not base_key:
                    log_append(text_area, logs, f"[RULE3] Base variable '{base_raw}' not in allowed_vars. Skipping.")
                    continue

                # Service-level re-eval first
                if base_key in ("alpha_service", "beta_service", "gamma_service"):
                    sector = base_key.split('_')[0]
                    img1 = next((p for p in images_by_sector[sector] if Path(p).stem.endswith("_image_1")), None)
                    img2 = next((p for p in images_by_sector[sector] if Path(p).stem.endswith("_image_2")), None)
                    if img1 and img2:
                        log_append(text_area, logs, f"[RULE3] Re-evaluating service images for sector '{sector}' (strict).")
                        svc_eval = evaluate_service_images(token, img1, img2, MODEL_SERVICE, logs, text_area)
                        if svc_eval:
                            allowed_vars[base_key].update(svc_eval)
                            resolved_after = resolve_expression_with_vars(expr, allowed_vars)
                            if resolved_after is not None:
                                keys = key_pattern.findall(rest)
                                set_nested_value_case_insensitive(allowed_vars[base_key], keys, resolved_after)
                                if isinstance(resolved_after, (int, float)):
                                    cell.value = resolved_after
                                elif isinstance(resolved_after, str):
                                    cell.value = resolved_after
                                else:
                                    try:
                                        cell.value = json.dumps(resolved_after)
                                    except Exception:
                                        cell.value = str(resolved_after)
                                remapped_count += 1
                                continue
                    # fall back to asking the model using JSON var
                    log_append(text_area, logs, f"[RULE3] Asking model for value of '{expr}' using variable '{base_key}'.")
                    value = ask_model_for_expression_value(token, base_key, allowed_vars[base_key], expr, MODEL_GENERIC)
                    if value is not None:
                        keys = key_pattern.findall(rest)
                        set_nested_value_case_insensitive(allowed_vars[base_key], keys, value)
                        if isinstance(value, (int, float)):
                            cell.value = value
                        elif isinstance(value, str):
                            cell.value = value
                        else:
                            try:
                                cell.value = json.dumps(value)
                            except Exception:
                                cell.value = str(value)
                        remapped_count += 1
                        continue
                    else:
                        log_append(text_area, logs, f"[RULE3] Model could not produce value for '{expr}'. Leaving NULL.")
                        continue

                # Non-service variables: find image key and re-evaluate or ask model
                keys = key_pattern.findall(rest)
                if not keys:
                    log_append(text_area, logs, f"[RULE3] No bracketed keys in '{expr}', cannot remap. Skipping.")
                    continue
                image_key = keys[0]
                file_path = None
                for lst in images_by_sector.values():
                    for p in lst:
                        if Path(p).stem == image_key:
                            file_path = p
                            break
                    if file_path:
                        break

                if not file_path:
                    log_append(text_area, logs, f"[RULE3] Could not find file for image '{image_key}'. Asking model with variable '{base_key}'.")
                    value = ask_model_for_expression_value(token, base_key, allowed_vars[base_key], expr, MODEL_GENERIC)
                    if value is not None:
                        set_nested_value_case_insensitive(allowed_vars[base_key], keys[1:], value)
                        if isinstance(value, (int, float)):
                            cell.value = value
                        elif isinstance(value, str):
                            cell.value = value
                        else:
                            try:
                                cell.value = json.dumps(value)
                            except Exception:
                                cell.value = str(value)
                        remapped_count += 1
                    else:
                        log_append(text_area, logs, f"[RULE3] Could not remap '{expr}'.")
                    continue

                # file exists -> strict evaluate image
                if image_key.startswith("voicetest"):
                    log_append(text_area, logs, f"[RULE3] Strictly evaluating voice image '{image_key}'.")
                    voice_eval = evaluate_voice_image(file_path, MODEL_GENERIC, logs, text_area)
                    if voice_eval and 'data' in voice_eval:
                        voice_test.setdefault(image_key, {}).update(voice_eval['data'])
                        nested_keys = keys[1:]
                        resolved_after = resolve_expression_with_vars(expr, {**allowed_vars, "voice_test": voice_test})
                        if resolved_after is not None:
                            set_nested_value_case_insensitive(voice_test, nested_keys, resolved_after)
                            if isinstance(resolved_after, (int, float)):
                                cell.value = resolved_after
                            elif isinstance(resolved_after, str):
                                cell.value = resolved_after
                            else:
                                try:
                                    cell.value = json.dumps(resolved_after)
                                except Exception:
                                    cell.value = str(resolved_after)
                            remapped_count += 1
                            continue
                    log_append(text_area, logs, f"[RULE3] Asking model for value of '{expr}' using 'voice_test' variable.")
                    value = ask_model_for_expression_value(token, "voice_test", voice_test, expr, MODEL_GENERIC)
                    if value is not None:
                        set_nested_value_case_insensitive(voice_test, keys[1:], value)
                        cell.value = value if not isinstance(value, dict) else json.dumps(value)
                        remapped_count += 1
                    else:
                        log_append(text_area, logs, f"[RULE3] Could not remap '{expr}' from voice image.")
                    continue
                else:
                    log_append(text_area, logs, f"[RULE3] Strictly evaluating generic image '{image_key}'.")
                    gen_eval = evaluate_generic_image(token, file_path, MODEL_GENERIC, logs, text_area)
                    if gen_eval and 'data' in gen_eval:
                        pref = image_key.split('_')[0]
                        if pref == "alpha":
                            if gen_eval['image_type'] == 'speed_test':
                                alpha_speedtest.setdefault(image_key, {}).update(gen_eval['data'])
                            elif gen_eval['image_type'] == 'video_test':
                                alpha_video.setdefault(image_key, {}).update(gen_eval['data'])
                        elif pref == "beta":
                            if gen_eval['image_type'] == 'speed_test':
                                beta_speedtest.setdefault(image_key, {}).update(gen_eval['data'])
                            elif gen_eval['image_type'] == 'video_test':
                                beta_video.setdefault(image_key, {}).update(gen_eval['data'])
                        elif pref == "gamma":
                            if gen_eval['image_type'] == 'speed_test':
                                gamma_speedtest.setdefault(image_key, {}).update(gen_eval['data'])
                            elif gen_eval['image_type'] == 'video_test':
                                gamma_video.setdefault(image_key, {}).update(gen_eval['data'])

                        new_allowed = {
                            "alpha_service": alpha_service, "beta_service": beta_service, "gamma_service": gamma_service,
                            "alpha_speedtest": alpha_speedtest, "beta_speedtest": beta_speedtest, "gamma_speedtest": gamma_speedtest,
                            "alpha_video": alpha_video, "beta_video": beta_video, "gamma_video": gamma_video,
                            "voice_test": voice_test, "avearge": avearge
                        }
                        resolved_after = resolve_expression_with_vars(expr, new_allowed)
                        if resolved_after is not None:
                            nested_keys = key_pattern.findall(rest)
                            if base_key in new_allowed:
                                set_nested_value_case_insensitive(new_allowed[base_key], nested_keys, resolved_after)
                            if isinstance(resolved_after, (int, float)):
                                cell.value = resolved_after
                            elif isinstance(resolved_after, str):
                                cell.value = resolved_after
                            else:
                                try:
                                    cell.value = json.dumps(resolved_after)
                                except Exception:
                                    cell.value = str(resolved_after)
                            remapped_count += 1
                            continue

                    log_append(text_area, logs, f"[RULE3] Asking model for value of '{expr}' using variable '{base_key}'.")
                    value = ask_model_for_expression_value(token, base_key, allowed_vars[base_key], expr, MODEL_GENERIC)
                    if value is not None:
                        nested_keys = key_pattern.findall(rest)
                        set_nested_value_case_insensitive(allowed_vars[base_key], nested_keys, value)
                        if isinstance(value, (int, float)):
                            cell.value = value
                        elif isinstance(value, str):
                            cell.value = value
                        else:
                            try:
                                cell.value = json.dumps(value)
                            except Exception:
                                cell.value = str(value)
                        remapped_count += 1
                    else:
                        log_append(text_area, logs, f"[RULE3] Could not remap '{expr}'. Left as NULL.")

            wb_r3.save(user_path)
            log_append(text_area, logs, f"[RULE3] Remapping complete. Cells remapped: {remapped_count}. Workbook saved.")

        except Exception as e:
            log_append(text_area, logs, f"[ERROR] Rule 3 remapping failed: {e}")

        # FINAL outputs shown in logs (also saved workbook updated earlier)
        log_append(text_area, logs, "\n" + "="*40)
        log_append(text_area, logs, "--- FINAL STRUCTURED DATA ---")
        try:
            log_append(text_area, logs, json.dumps({
                "alpha_service": alpha_service,
                "beta_service": beta_service,
                "gamma_service": gamma_service,
                "alpha_speedtest": alpha_speedtest,
                "beta_speedtest": beta_speedtest,
                "gamma_speedtest": gamma_speedtest,
                "alpha_video": alpha_video,
                "beta_video": beta_video,
                "gamma_video": gamma_video,
                "voice_test": voice_test,
                "avearge": avearge
            }, indent=2))
        except Exception:
            pass

        return user_path

    finally:
        log_append(text_area, logs, "\n[LOG] Processing complete (temporary files remain until app session ends).")

# -------------------- Streamlit UI --------------------
def main_ui():
    st.set_page_config(page_title="Cellular Template Processor", layout="wide")
    st.title("Advanced Cellular Template Processor — Streamlit UI")

    # Left sidebar: API key
    st.sidebar.header("API Key & Settings")
    api_key = st.sidebar.text_input("Enter your Apify / OpenRouter API token", type="password", placeholder="apify_api_...")
    check_button = st.sidebar.button("Validate API key")

    # area to show logs
    logs = []
    log_area = st.empty()
    # initialize visible area with blank
    log_area.text("Logs will appear here...")

    # Validate button action
    if check_button:
        if not api_key:
            st.sidebar.error("Please enter an API key before validating.")
        else:
            ok, msg = validate_api_key(api_key)
            if ok:
                st.sidebar.success("API key looks valid.")
                st.session_state['APIFY_TOKEN'] = api_key
            else:
                st.sidebar.error(f"Validation failed: {msg}")

    if 'APIFY_TOKEN' in st.session_state and st.session_state['APIFY_TOKEN']:
        st.sidebar.info("API key is set. You can upload a file now.")
    else:
        st.sidebar.warning("Please enter and validate API key before processing files.")

    # Main: Upload file
    st.header("1) Upload template file (.xlsx or .csv)")
    uploaded = st.file_uploader("Upload .xlsx or .csv (if CSV, upload images below)", type=['xlsx', 'csv'], accept_multiple_files=False)

    st.markdown("---")
    st.header("2) (CSV only) Upload images or a zip of images")
    uploaded_images = st.file_uploader("Upload images (multiple) or a ZIP file", type=['png', 'jpg', 'jpeg', 'zip'], accept_multiple_files=True)

    st.markdown("---")
    process_btn = st.button("Process file")

    # temp dir per session
    if 'tmp_dir' not in st.session_state:
        st.session_state['tmp_dir'] = tempfile.mkdtemp(prefix="cellproc_")

    tmp_dir = st.session_state['tmp_dir']

    if process_btn:
        if 'APIFY_TOKEN' not in st.session_state or not st.session_state['APIFY_TOKEN']:
            st.error("Please set and validate your API key in the sidebar first.")
        elif not uploaded:
            st.error("Please upload a .xlsx or .csv file to process.")
        else:
            # save uploaded file to temp dir
            filename = uploaded.name
            user_path = os.path.join(tmp_dir, filename)
            with open(user_path, "wb") as f:
                f.write(uploaded.getbuffer())

            # if CSV, handle images: if uploaded_images contains a zip, extract; else use images list
            image_files_objs = []
            if filename.lower().endswith(".csv"):
                if not uploaded_images:
                    st.error("CSV input requires image files to be uploaded. Please upload images or a zip file.")
                else:
                    # detect if one of uploaded_images is a zip; if so extract it
                    for uf in uploaded_images:
                        if uf.name.lower().endswith(".zip"):
                            try:
                                zip_path = os.path.join(tmp_dir, uf.name)
                                with open(zip_path, "wb") as outzip:
                                    outzip.write(uf.getbuffer())
                                with ZipFile(zip_path, 'r') as z:
                                    z.extractall(tmp_dir)
                                st.success(f"Extracted zip {uf.name} into temp dir.")
                            except Exception as e:
                                st.warning(f"Failed to extract zip {uf.name}: {e}")
                        else:
                            image_files_objs.append(uf)

            # prepare UI log area
            log_area.text("Starting processing...")
            # call processing
            out_path = process_file_streamlit(user_path=user_path, token=st.session_state['APIFY_TOKEN'], temp_dir=tmp_dir, image_uploads=image_files_objs, logs=logs, text_area=log_area)
            if out_path:
                st.success("Processing complete. Download the updated file below.")
                # provide download button for updated workbook
                try:
                    with open(out_path, "rb") as final_f:
                        data_bytes = final_f.read()
                    st.download_button("Download updated workbook", data=data_bytes, file_name=f"updated_{Path(out_path).name}", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                except Exception as e:
                    st.error(f"Could not prepare download: {e}")

    # small cleanup button
    if st.sidebar.button("Cleanup temp files"):
        if 'tmp_dir' in st.session_state:
            try:
                shutil.rmtree(st.session_state['tmp_dir'])
            except Exception:
                pass
            st.session_state['tmp_dir'] = tempfile.mkdtemp(prefix="cellproc_")
            st.sidebar.success("Temporary files removed.")

if __name__ == "__main__":
    main_ui()
