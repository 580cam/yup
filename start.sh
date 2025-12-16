#!/bin/bash
set -e

echo "Installing Playwright browsers..."
playwright install --with-deps chromium

echo "Starting gunicorn..."
exec gunicorn app:app
