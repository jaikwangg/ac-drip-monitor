// Copy this file to secrets.h (same folder) and fill in real values.
// secrets.h is listed in .gitignore - never commit it.
#pragma once

// 2.4 GHz WiFi only (ESP32 has no 5 GHz). Prefer a separate IoT SSID/VLAN.
#define WIFI_SSID "REPLACE_WITH_SSID"
#define WIFI_PASS "REPLACE_WITH_PASSWORD"

// From @BotFather, e.g. "1234567890:AAH...". Keep it secret.
#define BOT_TOKEN "REPLACE_WITH_BOT_TOKEN"

// Group chat id from https://api.telegram.org/bot<TOKEN>/getUpdates
// Group ids are negative, e.g. "-1001234567890" (keep the minus sign).
#define CHAT_ID "REPLACE_WITH_CHAT_ID"
