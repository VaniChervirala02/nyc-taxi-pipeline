FROM quay.io/astronomer/astro-runtime:12.0.0

COPY packages.txt .
COPY requirements.txt .
