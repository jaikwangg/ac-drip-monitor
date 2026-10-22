// =============================================================================
// AC Drip Monitor - server room water-leak alert (ESP32 -> Telegram)
//
// Board : ESP32 DevKit V1 30-pin (Arduino core "esp32 by Espressif Systems",
//         board "ESP32 Dev Module")
// Inputs: 2 rain/water modules (pad + LM393 board), analog AO only.
//         Sensor VCC is driven from a GPIO and powered only while reading,
//         which slows electrolytic corrosion of the pad.
// Output: Telegram group messages over HTTPS, on-board LED (GPIO2) blinks
//         while water is detected.
//
// Before building: copy secrets.example.h to secrets.h and fill it in.
// Calibration: open Serial Monitor at 115200 baud, note the raw value of each
// sensor dry and wet, set threshold to the midpoint (see README.md).
// =============================================================================

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <esp_task_wdt.h>
#include <esp_system.h>
#include <time.h>
#include <sys/time.h>

#if __has_include("secrets.h")
#include "secrets.h"
#else
#error "secrets.h not found: copy secrets.example.h to secrets.h and fill in WIFI_SSID, WIFI_PASS, BOT_TOKEN, CHAT_ID"
#endif

#include "telegram_ca.h"

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

// Shown in every Telegram message so the group knows which board is talking.
static const char DEVICE_NAME[] = "AC Drip Monitor · ห้อง Server 5G";

struct SensorConfig {
  const char *name;     // label used in messages and on Serial
  uint8_t pinPower;     // GPIO that powers the sensor (HIGH only while reading)
  uint8_t pinAdc;       // analog input, MUST be an ADC1 pin (32-39)
  bool wetWhenLow;      // true: raw < threshold means wet (rain module)
  uint16_t threshold;   // 0-4095, midpoint between measured dry and wet raw
};

// Rain modules read HIGH (~4095) when dry and LOW when wet.
// Planned upgrade for sensor 2 (two bare parallel wires along the wall/rack):
//   wire A -> GPIO26, wire B -> GPIO35, 100k from GPIO35 to GND,
//   then set wetWhenLow = false and re-calibrate the threshold.
static constexpr SensorConfig SENSORS[] = {
  // name                 pinPower pinAdc wetWhenLow threshold
  { "① ใต้แอร์",            25,      34,    true,      2000 },
  { "② พื้นจุดต่ำข้าง rack",  26,      35,    true,      2000 },
};
static const size_t NUM_SENSORS = sizeof(SENSORS) / sizeof(SENSORS[0]);

static const uint8_t PIN_LED = 2;  // on-board blue LED, active HIGH

// Timing (milliseconds unless noted)
static const uint32_t READ_INTERVAL_MS      = 5000;      // read both sensors every 5 s
static const uint32_t SENSOR_SETTLE_MS      = 20;        // after powering a sensor
static const uint8_t  ADC_SAMPLES           = 8;         // averaged per reading
static const uint8_t  DEBOUNCE_READS        = 2;         // consecutive reads to change state
static const uint32_t REPEAT_WET_MS         = 60UL * 60UL * 1000UL;  // repeat alert while wet
static const uint8_t  HEARTBEAT_HOUR        = 8;         // daily heartbeat, local time (ICT)
static const uint8_t  HEARTBEAT_MINUTE      = 30;
static const uint16_t HEARTBEAT_WINDOW_MIN  = 60;        // send if first possible within 08:30-09:30
static const uint32_t WIFI_RETRY_MS         = 30UL * 1000UL;
static const uint32_t WIFI_REBOOT_AFTER_MS  = 10UL * 60UL * 1000UL;  // reboot if WiFi down this long
static const uint32_t SEND_BACKOFF_MIN_MS   = 15UL * 1000UL;   // first retry after a failed send
static const uint32_t SEND_BACKOFF_MAX_MS   = 5UL * 60UL * 1000UL;
static const uint32_t HTTP_CONNECT_TIMEOUT_MS = 10000;
static const uint32_t HTTP_RESPONSE_TIMEOUT_MS = 10000;
static const uint32_t TLS_HANDSHAKE_TIMEOUT_S = 15;
// Worst case for one send is roughly DNS + connect + handshake + response
// (< 50 s), so the watchdog must be comfortably longer than that.
static const uint32_t WDT_TIMEOUT_S         = 90;
static const uint32_t LED_BLINK_MS          = 125;       // 4 Hz blink while wet

// Time: ICT = UTC+7, no DST. POSIX TZ strings use the inverted sign.
static const char TZ_INFO[] = "ICT-7";
static const char NTP_1[] = "pool.ntp.org";
static const char NTP_2[] = "time.google.com";
static const char NTP_3[] = "time.cloudflare.com";

// Pending Telegram messages kept across WiFi outages (and software reboots).
static const uint8_t  QUEUE_CAP   = 6;
static const uint16_t MSG_MAX     = 512;   // bytes, UTF-8 (Thai is 3 bytes/char)

// -----------------------------------------------------------------------------
// State
// -----------------------------------------------------------------------------

struct SensorState {
  uint16_t raw = 0;          // last averaged reading
  bool wetNow = false;       // last single reading compared with threshold
  bool wet = false;          // debounced state
  uint8_t wetStreak = 0;
  uint8_t dryStreak = 0;
  uint32_t wetSinceMs = 0;
  uint32_t lastRepeatMs = 0;
  time_t wetSinceEpoch = 0;  // 0 if the clock was not set at detection
  char wetSinceText[96] = "";
};
static SensorState g_sensor[NUM_SENSORS];

enum EventType : uint8_t { EV_ALERT = 1, EV_CLEAR, EV_REPEAT, EV_HEARTBEAT };
enum SendResult { SEND_OK, SEND_RETRY, SEND_DROP, SEND_RETRY_NOW };

struct Event {
  uint8_t type;
  uint8_t sensor;
  char text[MSG_MAX];
};

// Lives in RTC slow memory and survives esp_restart() and watchdog resets
// (not power loss), so a pending alert is not lost when the board reboots
// itself after a long WiFi outage. Validated by magic + checksum on boot.
struct Persist {
  uint32_t magic;
  uint32_t size;
  uint8_t count;
  uint8_t pad[3];
  uint32_t dropped;          // messages discarded because the queue was full
  int32_t lastHeartbeatYmd;  // e.g. 20261022, avoids a second heartbeat after a reboot
  char chatIdOverride[24];   // set if Telegram reports the group became a supergroup
  // Debounced wet state, so a reboot neither repeats the alert nor loses the later "back to normal".
  uint8_t wetFlags;          // bit i = sensor i wet
  int64_t wetSinceEpoch[NUM_SENSORS];
  char wetSinceText[NUM_SENSORS][96];
  Event q[QUEUE_CAP];
  uint32_t checksum;
};
static const uint32_t PERSIST_MAGIC = 0xAC0D1201;
static RTC_NOINIT_ATTR Persist g_persist;

static bool g_bootMsgPending = true;
static uint8_t g_restoredCount = 0;
static esp_reset_reason_t g_resetReason = ESP_RST_UNKNOWN;

static bool g_wifiUp = false;
static uint32_t g_wifiDownSinceMs = 0;
static uint32_t g_lastWifiAttemptMs = 0;

static uint32_t g_lastReadMs = 0;
static bool g_firstRead = true;
static uint32_t g_nextSendMs = 0;
static uint32_t g_sendBackoffMs = SEND_BACKOFF_MIN_MS;

static uint32_t g_lastLedMs = 0;
static bool g_ledOn = false;

// -----------------------------------------------------------------------------
// Compile-time sanity checks
// (Kept below all type definitions: the Arduino builder inserts function
// prototypes just before the first function, so types must come first.)
// -----------------------------------------------------------------------------

// ADC2 pins stop working while WiFi is on, so only ADC1 (GPIO32-39) is allowed.
constexpr bool isAdc1Pin(uint8_t p) {
  return p == 32 || p == 33 || p == 34 || p == 35 || p == 36 || p == 39;
}
static_assert(sizeof(SENSORS) / sizeof(SENSORS[0]) == 2, "README and wiring assume two sensors");
static_assert(isAdc1Pin(SENSORS[0].pinAdc) && isAdc1Pin(SENSORS[1].pinAdc),
              "sensor AO pins must be ADC1 (GPIO32-39); ADC2 fails while WiFi is on");
static_assert(DEBOUNCE_READS >= 1, "DEBOUNCE_READS must be >= 1");
static_assert(HEARTBEAT_HOUR < 24 && HEARTBEAT_MINUTE < 60, "bad heartbeat time");

// -----------------------------------------------------------------------------
// Small helpers
// -----------------------------------------------------------------------------

// Wrap-safe "has deadline passed" for millis() values.
static inline bool timeReached(uint32_t now, uint32_t deadline) {
  return (int32_t)(now - deadline) >= 0;
}

static bool timeValid() {
  return time(nullptr) > 1704067200;  // after 2024-01-01: clock has been set
}

// "22/10/2026 14:05:31" in ICT, or uptime if the clock is not set yet.
static void formatNow(char *buf, size_t len) {
  if (timeValid()) {
    time_t t = time(nullptr);
    struct tm lt;
    localtime_r(&t, &lt);
    strftime(buf, len, "%d/%m/%Y %H:%M:%S", &lt);
  } else {
    snprintf(buf, len, "เวลายังไม่ซิงก์ (uptime %lus)", (unsigned long)(millis() / 1000));
  }
}

static void formatDuration(uint32_t ms, char *buf, size_t len) {
  uint32_t mins = ms / 60000UL;
  uint32_t days = mins / 1440, hours = (mins / 60) % 24, m = mins % 60;
  if (days)       snprintf(buf, len, "%lu วัน %lu ชม. %lu นาที", (unsigned long)days, (unsigned long)hours, (unsigned long)m);
  else if (hours) snprintf(buf, len, "%lu ชม. %lu นาที", (unsigned long)hours, (unsigned long)m);
  else            snprintf(buf, len, "%lu นาที", (unsigned long)m);
}

// Cuts an incomplete multi-byte UTF-8 sequence left at the end by truncation
// (Telegram rejects invalid UTF-8).
static void utf8TrimTail(char *s) {
  size_t n = strlen(s);
  if (n == 0) return;
  size_t i = n;
  while (i > 0 && (((uint8_t)s[i - 1]) & 0xC0) == 0x80) i--;  // continuation bytes
  if (i == 0) return;
  uint8_t lead = (uint8_t)s[i - 1];
  size_t need = (lead & 0x80) == 0 ? 1 : (lead & 0xE0) == 0xC0 ? 2 : (lead & 0xF0) == 0xE0 ? 3 : 4;
  if (n - (i - 1) < need) s[i - 1] = '\0';
}

// Wet duration: wall clock if both ends are known, else millis().
static uint32_t wetDurationMs(const SensorState &s, uint32_t now) {
  if (s.wetSinceEpoch && timeValid()) {
    time_t d = time(nullptr) - s.wetSinceEpoch;
    if (d >= 0 && d < 40L * 86400L) return (uint32_t)d * 1000UL;  // fits in uint32 ms
  }
  return now - s.wetSinceMs;
}

static const char *resetReasonText(esp_reset_reason_t r) {
  switch (r) {
    case ESP_RST_POWERON:  return "ไฟเข้า (power-on)";
    case ESP_RST_SW:       return "รีบูตโดยโปรแกรม (เช่น WiFi หลุดนาน)";
    case ESP_RST_PANIC:    return "โปรแกรมขัดข้อง (panic)";
    case ESP_RST_INT_WDT:
    case ESP_RST_TASK_WDT:
    case ESP_RST_WDT:      return "watchdog รีเซ็ต (บอร์ดค้าง)";
    case ESP_RST_BROWNOUT: return "ไฟตก (brownout)";
    case ESP_RST_EXT:      return "กดปุ่ม EN/รีเซ็ต";
    case ESP_RST_DEEPSLEEP:return "ตื่นจาก deep sleep";
    default:               return "ไม่ทราบ";
  }
}

static const char *chatId() {
  return g_persist.chatIdOverride[0] ? g_persist.chatIdOverride : CHAT_ID;
}

static bool isWetReading(const SensorConfig &c, uint16_t raw) {
  return c.wetWhenLow ? (raw < c.threshold) : (raw > c.threshold);
}

// -----------------------------------------------------------------------------
// Persistent message queue
// -----------------------------------------------------------------------------

static uint32_t persistChecksum() {
  // FNV-1a over everything except the checksum field.
  const uint8_t *p = reinterpret_cast<const uint8_t *>(&g_persist);
  size_t n = offsetof(Persist, checksum);
  uint32_t h = 2166136261UL;
  for (size_t i = 0; i < n; i++) { h ^= p[i]; h *= 16777619UL; }
  return h;
}

static void persistSeal() { g_persist.checksum = persistChecksum(); }

static void persistWetState() {
  g_persist.wetFlags = 0;
  for (size_t i = 0; i < NUM_SENSORS; i++) {
    if (g_sensor[i].wet) g_persist.wetFlags |= (1u << i);
    g_persist.wetSinceEpoch[i] = g_sensor[i].wetSinceEpoch;
    strlcpy(g_persist.wetSinceText[i], g_sensor[i].wetSinceText, sizeof(g_persist.wetSinceText[i]));
  }
  persistSeal();
}

static void persistInit() {
  bool ok = g_persist.magic == PERSIST_MAGIC && g_persist.size == sizeof(Persist) &&
            g_persist.count <= QUEUE_CAP && g_persist.checksum == persistChecksum();
  if (!ok) {
    memset(&g_persist, 0, sizeof(g_persist));
    g_persist.magic = PERSIST_MAGIC;
    g_persist.size = sizeof(Persist);
  }
  // Force NUL termination in case of any corruption that kept the checksum.
  for (uint8_t i = 0; i < QUEUE_CAP; i++) g_persist.q[i].text[MSG_MAX - 1] = '\0';
  g_persist.chatIdOverride[sizeof(g_persist.chatIdOverride) - 1] = '\0';
  g_restoredCount = g_persist.count;
  uint32_t now = millis();
  for (size_t i = 0; i < NUM_SENSORS; i++) {
    g_persist.wetSinceText[i][sizeof(g_persist.wetSinceText[i]) - 1] = '\0';
    if (g_persist.wetFlags & (1u << i)) {
      SensorState &s = g_sensor[i];
      s.wet = true;
      s.wetSinceMs = now;
      s.lastRepeatMs = now;
      s.wetSinceEpoch = (time_t)g_persist.wetSinceEpoch[i];
      strlcpy(s.wetSinceText, g_persist.wetSinceText[i], sizeof(s.wetSinceText));
      Serial.printf("[persist] %s was wet before reboot\n", SENSORS[i].name);
    }
  }
  persistSeal();
}

static void queueRemove(uint8_t idx) {
  if (idx >= g_persist.count) return;
  for (uint8_t i = idx; i + 1 < g_persist.count; i++) g_persist.q[i] = g_persist.q[i + 1];
  g_persist.count--;
  persistSeal();
}

static void queuePush(EventType type, uint8_t sensor, const char *text) {
  // A newer reminder for the same sensor replaces an unsent older one.
  if (type == EV_REPEAT) {
    for (uint8_t i = 0; i < g_persist.count; i++) {
      Event &e = g_persist.q[i];
      if (e.type == EV_REPEAT && e.sensor == sensor) {
        strlcpy(e.text, text, MSG_MAX);
        utf8TrimTail(e.text);
        persistSeal();
        return;
      }
    }
  }
  if (g_persist.count >= QUEUE_CAP) {
    // Full: drop the oldest non-alert message; only drop an alert if all are alerts.
    uint8_t victim = 0;
    for (uint8_t i = 0; i < g_persist.count; i++) {
      if (g_persist.q[i].type != EV_ALERT) { victim = i; break; }
    }
    Serial.printf("[queue] full, dropping message %u (type %u)\n", victim, g_persist.q[victim].type);
    queueRemove(victim);
    g_persist.dropped++;
  }
  Event &e = g_persist.q[g_persist.count];
  e.type = type;
  e.sensor = sensor;
  strlcpy(e.text, text, MSG_MAX);
  utf8TrimTail(e.text);
  g_persist.count++;
  persistSeal();
  Serial.printf("[queue] +type %u, pending %u\n", type, g_persist.count);
}

// -----------------------------------------------------------------------------
// Clock fallback: use the HTTP Date header if NTP (UDP 123) is blocked
// -----------------------------------------------------------------------------

static int64_t daysFromCivil(int y, unsigned m, unsigned d) {
  // Howard Hinnant's algorithm, proleptic Gregorian calendar.
  y -= m <= 2;
  const int era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = (unsigned)(y - era * 400);
  const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return (int64_t)era * 146097 + (int64_t)doe - 719468;
}

// Parses "Wed, 22 Oct 2026 01:30:00 GMT" and sets the clock if not set yet.
static void setClockFromHttpDate(const String &date) {
  if (timeValid() || date.length() < 25) return;
  int d, y, hh, mm, ss;
  char mon[4] = {0};
  if (sscanf(date.c_str(), "%*3s, %d %3s %d %d:%d:%d", &d, mon, &y, &hh, &mm, &ss) != 6) return;
  static const char MONTHS[] = "JanFebMarAprMayJunJulAugSepOctNovDec";
  const char *p = strstr(MONTHS, mon);
  if (!p || mon[0] == '\0') return;
  unsigned m = (unsigned)((p - MONTHS) / 3) + 1;
  int64_t epoch = daysFromCivil(y, m, (unsigned)d) * 86400LL + hh * 3600LL + mm * 60LL + ss;
  struct timeval tv = { (time_t)epoch, 0 };
  settimeofday(&tv, nullptr);
  Serial.printf("[time] clock set from HTTP Date header: %s\n", date.c_str());
}

// -----------------------------------------------------------------------------
// Telegram
// -----------------------------------------------------------------------------

static void urlEncodeAppend(String &out, const char *s) {
  static const char HEX_DIGITS[] = "0123456789ABCDEF";
  for (; *s; s++) {
    uint8_t c = (uint8_t)*s;
    if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      out += (char)c;
    } else {
      out += '%';
      out += HEX_DIGITS[c >> 4];
      out += HEX_DIGITS[c & 0x0F];
    }
  }
}

// Extracts an integer field like "retry_after":30 from a JSON body (no JSON lib needed).
static bool jsonInt(const String &body, const char *key, long long &out) {
  String k = String("\"") + key + "\":";
  int i = body.indexOf(k);
  if (i < 0) return false;
  out = atoll(body.c_str() + i + k.length());
  return true;
}

// One blocking HTTPS POST to sendMessage, bounded by the timeouts above.
static SendResult telegramSend(const char *text, uint32_t &retryAfterMs) {
  retryAfterMs = 0;
  WiFiClientSecure tls;
  tls.setCACert(TELEGRAM_CA_BUNDLE);
  tls.setHandshakeTimeout(TLS_HANDSHAKE_TIMEOUT_S);

  HTTPClient http;
  http.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
  http.setTimeout(HTTP_RESPONSE_TIMEOUT_MS);
  http.setReuse(false);
  static const char *HEADER_KEYS[] = { "Date" };
  http.collectHeaders(HEADER_KEYS, 1);

  String url = String("https://api.telegram.org/bot") + BOT_TOKEN + "/sendMessage";
  if (!http.begin(tls, url)) {
    Serial.println("[tg] http.begin failed");
    return SEND_RETRY;
  }
  http.addHeader("Content-Type", "application/x-www-form-urlencoded");

  String body;
  body.reserve(strlen(text) * 3 + 96);
  body += "chat_id=";
  urlEncodeAppend(body, chatId());
  body += "&disable_web_page_preview=true&text=";
  urlEncodeAppend(body, text);

  esp_task_wdt_reset();
  int code = http.POST(body);
  esp_task_wdt_reset();

  if (code <= 0) {
    // Negative codes are transport errors (DNS, TCP, TLS, timeout).
    Serial.printf("[tg] transport error %d: %s\n", code, HTTPClient::errorToString(code).c_str());
    char tlsErr[100] = "";
    if (tls.lastError(tlsErr, sizeof(tlsErr)) != 0) Serial.printf("[tg] TLS: %s\n", tlsErr);
    http.end();
    return SEND_RETRY;
  }

  setClockFromHttpDate(http.header("Date"));
  String resp = http.getString();
  http.end();

  if (code == 200) {
    Serial.println("[tg] sent OK");
    return SEND_OK;
  }

  Serial.printf("[tg] HTTP %d: %.200s\n", code, resp.c_str());
  long long v;
  if (code == 429) {
    if (jsonInt(resp, "retry_after", v) && v > 0 && v < 3600) retryAfterMs = (uint32_t)v * 1000UL;
    return SEND_RETRY;
  }
  if (code == 400 && jsonInt(resp, "migrate_to_chat_id", v)) {
    // The group was upgraded to a supergroup and has a new id.
    snprintf(g_persist.chatIdOverride, sizeof(g_persist.chatIdOverride), "%lld", v);
    persistSeal();
    Serial.printf("[tg] group migrated: using chat id %s - UPDATE CHAT_ID in secrets.h\n",
                  g_persist.chatIdOverride);
    return SEND_RETRY_NOW;
  }
  if (code == 400) {
    // Malformed request: this exact message will never succeed, do not block the queue.
    return SEND_DROP;
  }
  // 401 bad token, 403 bot removed from group, 404, 5xx: keep and retry with backoff.
  return SEND_RETRY;
}

static void buildBootMessage(char *buf, size_t len) {
  char now[96];
  formatNow(now, sizeof(now));
  int n = snprintf(buf, len,
                   "🟢 %s เริ่มทำงาน\n"
                   "IP: %s\n"
                   "WiFi RSSI: %d dBm\n"
                   "สาเหตุที่เริ่มใหม่: %s\n"
                   "เวลา: %s",
                   DEVICE_NAME, WiFi.localIP().toString().c_str(), (int)WiFi.RSSI(),
                   resetReasonText(g_resetReason), now);
  if (g_restoredCount && n > 0 && (size_t)n < len) {
    snprintf(buf + n, len - n, "\nมีข้อความค้างส่งจากก่อนรีบูต %u ข้อความ (จะตามมา)", g_restoredCount);
  }
}

// Sends at most one message per call so sensor reads keep running.
static void serviceTelegram(uint32_t now) {
  if (!g_wifiUp || !timeReached(now, g_nextSendMs)) return;
  if (!g_bootMsgPending && g_persist.count == 0) return;

  static char bootBuf[MSG_MAX];
  const char *text;
  if (g_bootMsgPending) {
    buildBootMessage(bootBuf, sizeof(bootBuf));
    text = bootBuf;
  } else {
    text = g_persist.q[0].text;
  }

  uint32_t retryAfterMs = 0;
  SendResult r = telegramSend(text, retryAfterMs);
  uint32_t after = millis();

  switch (r) {
    case SEND_OK:
    case SEND_DROP:
      if (r == SEND_DROP) Serial.println("[tg] message rejected (400), dropped");
      if (g_bootMsgPending) g_bootMsgPending = false;
      else queueRemove(0);
      g_sendBackoffMs = SEND_BACKOFF_MIN_MS;
      g_nextSendMs = after + 3000;  // stay well under Telegram's 20 msg/min group limit
      break;
    case SEND_RETRY_NOW:
      g_nextSendMs = after;
      break;
    case SEND_RETRY:
      g_nextSendMs = after + (retryAfterMs ? retryAfterMs : g_sendBackoffMs);
      Serial.printf("[tg] retry in %lu s, pending %u\n",
                    (unsigned long)((retryAfterMs ? retryAfterMs : g_sendBackoffMs) / 1000),
                    g_persist.count + (g_bootMsgPending ? 1 : 0));
      g_sendBackoffMs = min(g_sendBackoffMs * 2, SEND_BACKOFF_MAX_MS);
      break;
  }
}

// -----------------------------------------------------------------------------
// Sensors
// -----------------------------------------------------------------------------

static uint16_t readSensorRaw(const SensorConfig &c) {
  digitalWrite(c.pinPower, HIGH);
  delay(SENSOR_SETTLE_MS);
  uint32_t sum = 0;
  for (uint8_t i = 0; i < ADC_SAMPLES; i++) sum += analogRead(c.pinAdc);
  digitalWrite(c.pinPower, LOW);
  return (uint16_t)(sum / ADC_SAMPLES);
}

static void onBecameWet(size_t i, uint32_t now) {
  const SensorConfig &c = SENSORS[i];
  SensorState &s = g_sensor[i];
  s.wetSinceMs = now;
  s.lastRepeatMs = now;
  s.wetSinceEpoch = timeValid() ? time(nullptr) : 0;
  formatNow(s.wetSinceText, sizeof(s.wetSinceText));
  persistWetState();
  char msg[MSG_MAX];
  snprintf(msg, sizeof(msg),
           "⚠️ ตรวจพบน้ำ! %s\n"
           "จุด: %s\n"
           "ค่า raw: %u (เกณฑ์ %s %u)\n"
           "เวลา: %s\n"
           "%s",
           DEVICE_NAME, c.name, s.raw, c.wetWhenLow ? "<" : ">", c.threshold, s.wetSinceText,
           "โปรดตรวจสอบแอร์/น้ำทิ้งโดยเร็ว");
  Serial.printf("[event] WET %s raw=%u\n", c.name, s.raw);
  queuePush(EV_ALERT, (uint8_t)i, msg);
}

static void onBecameDry(size_t i, uint32_t now) {
  const SensorConfig &c = SENSORS[i];
  SensorState &s = g_sensor[i];
  char tnow[96], dur[64], msg[MSG_MAX];
  formatNow(tnow, sizeof(tnow));
  formatDuration(wetDurationMs(s, now), dur, sizeof(dur));
  snprintf(msg, sizeof(msg),
           "✅ กลับเป็นปกติ (แห้งแล้ว) %s\n"
           "จุด: %s\n"
           "ค่า raw: %u\n"
           "พบน้ำตั้งแต่: %s (นาน %s)\n"
           "เวลา: %s",
           DEVICE_NAME, c.name, s.raw, s.wetSinceText, dur, tnow);
  persistWetState();
  Serial.printf("[event] DRY %s raw=%u\n", c.name, s.raw);
  queuePush(EV_CLEAR, (uint8_t)i, msg);
}

static void checkRepeat(size_t i, uint32_t now) {
  SensorState &s = g_sensor[i];
  if (!s.wet || (now - s.lastRepeatMs) < REPEAT_WET_MS) return;
  s.lastRepeatMs = now;
  char tnow[96], dur[64], msg[MSG_MAX];
  formatNow(tnow, sizeof(tnow));
  formatDuration(wetDurationMs(s, now), dur, sizeof(dur));
  snprintf(msg, sizeof(msg),
           "🔁 ยังพบน้ำอยู่ %s\n"
           "จุด: %s\n"
           "ค่า raw: %u\n"
           "พบน้ำต่อเนื่อง %s (ตั้งแต่ %s)\n"
           "เวลา: %s",
           DEVICE_NAME, SENSORS[i].name, s.raw, dur, s.wetSinceText, tnow);
  queuePush(EV_REPEAT, (uint8_t)i, msg);
}

static void readAllSensors(uint32_t now) {
  for (size_t i = 0; i < NUM_SENSORS; i++) {
    const SensorConfig &c = SENSORS[i];
    SensorState &s = g_sensor[i];
    s.raw = readSensorRaw(c);
    s.wetNow = isWetReading(c, s.raw);
    if (s.wetNow) { if (s.wetStreak < 255) s.wetStreak++; s.dryStreak = 0; }
    else          { if (s.dryStreak < 255) s.dryStreak++; s.wetStreak = 0; }

    if (!s.wet && s.wetStreak >= DEBOUNCE_READS) { s.wet = true; onBecameWet(i, now); }
    else if (s.wet && s.dryStreak >= DEBOUNCE_READS) { s.wet = false; onBecameDry(i, now); }
    checkRepeat(i, now);
  }

  // Calibration / diagnostics line on every read.
  Serial.printf("[read %6lus]", (unsigned long)(now / 1000));
  for (size_t i = 0; i < NUM_SENSORS; i++) {
    const SensorState &s = g_sensor[i];
    Serial.printf(" | %s raw=%4u th=%u -> %s", SENSORS[i].name, s.raw, SENSORS[i].threshold,
                  s.wet ? "WET" : (s.wetNow ? "wet?" : "dry"));
  }
  if (g_wifiUp) Serial.printf(" | WiFi %d dBm", (int)WiFi.RSSI());
  else          Serial.print(" | WiFi DOWN");
  Serial.printf(" | pending %u\n", g_persist.count + (g_bootMsgPending ? 1 : 0));
}

static bool anyWet() {
  for (size_t i = 0; i < NUM_SENSORS; i++) if (g_sensor[i].wet) return true;
  return false;
}

// -----------------------------------------------------------------------------
// Heartbeat (daily 08:30 ICT)
// -----------------------------------------------------------------------------

static void serviceHeartbeat(uint32_t now) {
  if (!timeValid()) return;  // never guess the time
  time_t t = time(nullptr);
  struct tm lt;
  localtime_r(&t, &lt);
  int32_t ymd = (lt.tm_year + 1900) * 10000 + (lt.tm_mon + 1) * 100 + lt.tm_mday;
  int minutes = lt.tm_hour * 60 + lt.tm_min;
  int target = HEARTBEAT_HOUR * 60 + HEARTBEAT_MINUTE;
  if (minutes < target || minutes >= target + HEARTBEAT_WINDOW_MIN) return;
  if (g_persist.lastHeartbeatYmd == ymd) return;

  g_persist.lastHeartbeatYmd = ymd;
  persistSeal();

  char tnow[96], up[64], msg[MSG_MAX];
  formatNow(tnow, sizeof(tnow));
  formatDuration(now, up, sizeof(up));
  int n = snprintf(msg, sizeof(msg), "💓 %s ยังทำงานปกติ\n", DEVICE_NAME);
  for (size_t i = 0; i < NUM_SENSORS && n > 0 && (size_t)n < sizeof(msg); i++) {
    n += snprintf(msg + n, sizeof(msg) - n, "%s: raw %u (%s)\n", SENSORS[i].name,
                  g_sensor[i].raw, g_sensor[i].wet ? "พบน้ำ" : "แห้ง");
  }
  if (n > 0 && (size_t)n < sizeof(msg)) {
    n += snprintf(msg + n, sizeof(msg) - n, "WiFi RSSI: %d dBm\nเปิดต่อเนื่อง: %s\nเวลา: %s",
                  (int)WiFi.RSSI(), up, tnow);
  }
  if (g_persist.dropped && n > 0 && (size_t)n < sizeof(msg)) {
    snprintf(msg + n, sizeof(msg) - n, "\nข้อความที่ทิ้งไปเพราะคิวเต็ม: %lu",
             (unsigned long)g_persist.dropped);
  }
  queuePush(EV_HEARTBEAT, 0xFF, msg);
}

// -----------------------------------------------------------------------------
// WiFi
// -----------------------------------------------------------------------------

static void wifiBegin(uint32_t now) {
  g_lastWifiAttemptMs = now;
  WiFi.disconnect();
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  Serial.printf("[wifi] connecting to \"%s\"...\n", WIFI_SSID);
}

static void serviceWifi(uint32_t now) {
  bool up = WiFi.status() == WL_CONNECTED;
  if (up && !g_wifiUp) {
    Serial.printf("[wifi] connected, IP %s, RSSI %d dBm\n",
                  WiFi.localIP().toString().c_str(), (int)WiFi.RSSI());
    g_nextSendMs = now;  // flush pending messages right away
    g_sendBackoffMs = SEND_BACKOFF_MIN_MS;
  } else if (!up && g_wifiUp) {
    Serial.println("[wifi] connection lost");
    g_wifiDownSinceMs = now;
    g_lastWifiAttemptMs = now - WIFI_RETRY_MS;  // retry immediately
  }
  g_wifiUp = up;
  if (up) return;

  if (now - g_wifiDownSinceMs >= WIFI_REBOOT_AFTER_MS) {
    Serial.println("[wifi] down for more than 10 min, rebooting (pending messages are kept)");
    Serial.flush();
    esp_restart();
  }
  if (now - g_lastWifiAttemptMs >= WIFI_RETRY_MS) wifiBegin(now);
}

// -----------------------------------------------------------------------------
// LED
// -----------------------------------------------------------------------------

static void serviceLed(uint32_t now) {
  if (anyWet()) {
    if (now - g_lastLedMs >= LED_BLINK_MS) {
      g_lastLedMs = now;
      g_ledOn = !g_ledOn;
      digitalWrite(PIN_LED, g_ledOn ? HIGH : LOW);
    }
  } else if (g_ledOn) {
    g_ledOn = false;
    digitalWrite(PIN_LED, LOW);
  }
}

// -----------------------------------------------------------------------------
// Watchdog
// -----------------------------------------------------------------------------

static void watchdogInit() {
#if ESP_ARDUINO_VERSION_MAJOR >= 3
  esp_task_wdt_config_t cfg = {};
  cfg.timeout_ms = WDT_TIMEOUT_S * 1000;
  cfg.idle_core_mask = (1 << portNUM_PROCESSORS) - 1;  // also watch both idle tasks
  cfg.trigger_panic = true;                            // reset the chip on timeout
  // The core usually starts the TWDT already; reconfigure it, or init if not.
  esp_err_t err = esp_task_wdt_reconfigure(&cfg);
  if (err == ESP_ERR_INVALID_STATE) err = esp_task_wdt_init(&cfg);
#else
  esp_err_t err = esp_task_wdt_init(WDT_TIMEOUT_S, true);
#endif
  if (err != ESP_OK) Serial.printf("[wdt] init error %d\n", (int)err);
  err = esp_task_wdt_add(nullptr);  // watch the loop task
  if (err != ESP_OK && err != ESP_ERR_INVALID_ARG) Serial.printf("[wdt] add error %d\n", (int)err);
}

// -----------------------------------------------------------------------------
// Arduino entry points
// -----------------------------------------------------------------------------

void setup() {
  Serial.begin(115200);
  delay(200);
  g_resetReason = esp_reset_reason();
  persistInit();

  Serial.println();
  Serial.println("==============================================");
  Serial.printf(" %s\n", DEVICE_NAME);
  Serial.printf(" reset reason: %d, restored pending messages: %u\n", (int)g_resetReason, g_restoredCount);
  Serial.println(" Calibration: note raw when DRY and when WET,");
  Serial.println(" set threshold = (dry + wet) / 2 for each sensor.");
  Serial.println("==============================================");
  if (strstr(BOT_TOKEN, "REPLACE") || strstr(CHAT_ID, "REPLACE")) {
    Serial.println("[config] WARNING: secrets.h still has placeholder values");
  }

  watchdogInit();

  pinMode(PIN_LED, OUTPUT);
  digitalWrite(PIN_LED, LOW);

  analogReadResolution(12);
  for (size_t i = 0; i < NUM_SENSORS; i++) {
    const SensorConfig &c = SENSORS[i];
    if (!isAdc1Pin(c.pinAdc)) Serial.printf("[config] ERROR: %s AO pin %u is not ADC1\n", c.name, c.pinAdc);
    pinMode(c.pinPower, OUTPUT);
    digitalWrite(c.pinPower, LOW);  // sensor unpowered between reads
    pinMode(c.pinAdc, INPUT);       // GPIO34-39 are input-only, no internal pulls
    analogSetPinAttenuation(c.pinAdc, ADC_11db);  // full 0-3.3 V range
  }

  WiFi.persistent(false);
  WiFi.setHostname("ac-drip-monitor");  // before mode() so DHCP uses it
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(false);  // reconnects are paced by serviceWifi()
  WiFi.setSleep(false);          // more reliable for an always-on alarm

  // SNTP runs in the background and retries on its own once WiFi is up.
  configTzTime(TZ_INFO, NTP_1, NTP_2, NTP_3);

  uint32_t now = millis();
  g_wifiDownSinceMs = now;
  wifiBegin(now);
}

void loop() {
  esp_task_wdt_reset();
  uint32_t now = millis();

  serviceWifi(now);

  if (g_firstRead || now - g_lastReadMs >= READ_INTERVAL_MS) {
    g_firstRead = false;
    g_lastReadMs = now;
    readAllSensors(now);
    serviceHeartbeat(now);
  }

  serviceTelegram(millis());
  serviceLed(millis());
  delay(10);  // yield to the WiFi/idle tasks; shorter than SENSOR_SETTLE_MS
}
