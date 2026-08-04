#!/usr/bin/env python3
"""Capture the Phase 2 workflow chart as a high-quality JPG."""
from playwright.sync_api import sync_playwright

src = "/Users/tarun/Desktop/Data Obs/insurance-pipeline-observability/assets/linkedin/phase2_workflow_chart.html"
out = "/Users/tarun/Desktop/Data Obs/insurance-pipeline-observability/assets/linkedin/phase2_workflow_chart.jpg"

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1200, "height": 800}, device_scale_factor=2)
    page.goto(f"file://{src}")
    page.wait_for_load_state("networkidle")
    # Let icon images finish loading
    page.wait_for_timeout(3000)
    page.screenshot(path=out, full_page=True, type="jpeg", quality=95)
    browser.close()

import os
size = os.path.getsize(out)
print(f"Saved {size:,} bytes → {out}")
