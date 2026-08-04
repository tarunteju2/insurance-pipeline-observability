#!/usr/bin/env python3
"""Capture the Phase 3 workflow chart as a high-quality JPG image."""

import os
from playwright.sync_api import sync_playwright

base_dir = os.path.dirname(os.path.abspath(__file__))
src = os.path.join(base_dir, "linkedin", "phase3_workflow_chart.html")
out = os.path.join(base_dir, "linkedin", "phase3_workflow_chart.jpg")

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1520, "height": 1200}, device_scale_factor=2)
    page.goto(f"file://{src}")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(2000)
    page.screenshot(path=out, full_page=True, type="jpeg", quality=95)
    browser.close()

size = os.path.getsize(out)
print(f"✅ Successfully captured Phase 3 Infographic JPG ({size:,} bytes) -> {out}")
