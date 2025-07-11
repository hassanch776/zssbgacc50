# Optimized but FUNCTIONAL build for Zillow Scrap
FROM ubuntu:22.04 AS base

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive \
    TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Install ALL necessary packages in one optimized layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    wget \
    gnupg \
    ca-certificates \
    # Chrome dependencies (ESSENTIAL - cannot remove!)
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libxss1 \
    libxtst6 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install Chrome (MUST keep - SeleniumBase needs it)
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /usr/share/keyrings/google-chrome-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# === PYTHON SETUP ===
# Create symlink for python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install Python packages with proper dependencies (NOT --no-deps!)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    # Verify installation
    python -c "import seleniumbase, requests, bs4, lxml; print('All packages working')"

# Set environment variables
ENV CHROME_BIN=/usr/bin/google-chrome \
    CHROME_PATH=/usr/bin/google-chrome

# Create workspace
WORKDIR /workspace
RUN useradd -m runner && chown runner:runner /workspace

# Quick verification (but don't pre-warm - that's what was slow!)
RUN google-chrome --version

CMD ["/bin/bash"] 
