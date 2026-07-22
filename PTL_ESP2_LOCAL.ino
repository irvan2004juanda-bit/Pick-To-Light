/*
 * ============================================
 *   PICK TO LIGHT — ESP32 #2 (PTL 5-8)
 *   FreeRTOS + MQTT (PubSubClient) + HTTP confirm
 *   PTL6/HMI2 DISABLED — modul OLED rusak (LDO mati)
 *   RELAY:  (aktif=HIGH, standby=LOW)
 *   CANCEL: handler ptl/cancel 
 * ============================================
 */

#include <WiFi.h>
#include <HTTPClient.h>
#include <ArduinoJson.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>

const char* WIFI_SSID     = "TP-Link_EBC6";
const char* WIFI_PASSWORD = "33460314";
const char* SERVER_IP     = "192.168.0.105";
const int   SERVER_PORT   = 5000;
const char* MQTT_BROKER   = "192.168.0.105";
const int   MQTT_PORT     = 1883;
String API_BASE = String("http://") + SERVER_IP + ":" + SERVER_PORT;

#define TOPIC_ORDER  "ptl/order"
#define TOPIC_CANCEL "ptl/cancel"

#define PTL1_LED_RED    15
#define PTL1_LED_GREEN  2
#define PTL2_LED_RED    5   // PTL6 - DISABLED
#define PTL2_LED_GREEN  18  // PTL6 - DISABLED
#define PTL3_LED_RED    13
#define PTL3_LED_GREEN  12
#define PTL4_LED_RED    32
#define PTL4_LED_GREEN  33
#define RELAY_PIN       25
#define PTL1_BTN        4
#define PTL2_BTN        19  // PTL6 - DISABLED
#define PTL3_BTN        14
#define PTL4_BTN        26
#define BTN_RESET       27
#define TCA_ADDR        0x70
#define OLED_CH_PTL1    1
#define OLED_CH_PTL2    0   // PTL6 - DISABLED
#define OLED_CH_PTL3    2
#define OLED_CH_PTL4    3
#define SCREEN_WIDTH  128
#define SCREEN_HEIGHT  64
#define OLED_RESET     -1
#define PTL_OFFSET      4

Adafruit_SSD1306 d1(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, OLED_RESET);
Adafruit_SSD1306 d2(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, OLED_RESET);
Adafruit_SSD1306 d3(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, OLED_RESET);
Adafruit_SSD1306 d4(SCREEN_WIDTH, SCREEN_HEIGHT, &Wire, OLED_RESET);

struct PTLState {
  String orderId   = "";
  String itemName  = "";
  String rackId    = "";
  int    qty       = 0;
  String orderId2  = "";
  String itemName2 = "";
  String rackId2   = "";
  int    qty2      = 0;
  bool   hasItem2     = false;
  bool   aktif        = false;
  bool   sedangKirim  = false;
  unsigned long timeOrderMasuk   = 0;
  unsigned long timeTombol       = 0;
  unsigned long timeKonfirmMulai = 0;
};

PTLState ptl1, ptl2, ptl3, ptl4;

struct KonfirmasiTask {
  int ptlNum;
  String orderId;
  String orderId2;
  bool   hasItem2;
  float respond_du;
  unsigned long timeOrderMasuk;
  unsigned long timeKonfirmMulai;
};

QueueHandle_t queue1, queue2, queue3, queue4;
SemaphoreHandle_t i2cMutex;
SemaphoreHandle_t ptlMutex;
SemaphoreHandle_t httpMutex;

WiFiClient   wifiClient;
PubSubClient mqttClient(wifiClient);

volatile bool flag1=false, flag2=false, flag3=false, flag4=false;
unsigned long lastDb1=0, lastDb2=0, lastDb3=0, lastDb4=0;
const long DEBOUNCE_DELAY = 50;
unsigned long lastMqttRetry = 0;

void tcaSelect(uint8_t channel);
Adafruit_SSD1306& getDisplay(int ptlNum);
int getOledChannel(int ptlNum);
PTLState& getPTL(int ptlNum);
QueueHandle_t getQueue(int ptlNum);
void setLED(int ptlNum, bool red, bool green);
void updateRelay();
void allLedOff();
void oledStandby(int ptlNum);
void oledOrder(int ptlNum, PTLState& ptl);
void oledSukses(int ptlNum);
void oledTampilkan(int ptlNum, const char* b1, const char* b2, const char* b3, const char* b4);
void handleTombol(int ptlNum);
void prosesKonfirmasi(KonfirmasiTask& task);
void reconnectMQTT();
void taskPTL1(void* p);
void taskPTL2(void* p);
void taskPTL3(void* p);
void taskPTL4(void* p);

void IRAM_ATTR isr1() { if(digitalRead(PTL1_BTN)==LOW) flag1=true; }
void IRAM_ATTR isr2() { if(digitalRead(PTL2_BTN)==LOW) flag2=true; }
void IRAM_ATTR isr3() { if(digitalRead(PTL3_BTN)==LOW) flag3=true; }
void IRAM_ATTR isr4() { if(digitalRead(PTL4_BTN)==LOW) flag4=true; }

int getRakPTL(String rackId) {
  if (rackId=="C-01"||rackId=="C-02") return 1;
  // if (rackId=="C-03"||rackId=="C-04") return 2;   // ⛔ PTL6 DISABLED — modul OLED rusak
  if (rackId=="D-01"||rackId=="D-02") return 3;
  if (rackId=="D-03"||rackId=="D-04") return 4;
  return 0;
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String msg = "";
  for (unsigned int i = 0; i < length; i++) msg += (char)payload[i];
  Serial.println("[MQTT] Pesan masuk: " + msg);

  // ── HANDLER CANCEL ──────────────────────── ✅ BARU (disamakan dengan ESP1)
  if (String(topic) == TOPIC_CANCEL) {
    DynamicJsonDocument docCancel(256);
    if (deserializeJson(docCancel, msg)) return;
    JsonArray ids = docCancel["order_ids"].as<JsonArray>();
    for (JsonVariant id : ids) {
      String cancelId = id.as<String>();
      for (int i = 1; i <= 4; i++) {
        if (i == 2) continue;   // ⛔ PTL6 DISABLED — skip, OLED-nya mati
        if (xSemaphoreTake(ptlMutex, pdMS_TO_TICKS(100)) == pdTRUE) {
          PTLState& ptl = getPTL(i);
          if (ptl.orderId == cancelId || ptl.orderId2 == cancelId) {
            ptl.orderId=""; ptl.itemName=""; ptl.rackId=""; ptl.qty=0;
            ptl.orderId2=""; ptl.itemName2=""; ptl.rackId2=""; ptl.qty2=0;
            ptl.hasItem2=false; ptl.aktif=false; ptl.sedangKirim=false;
            ptl.timeOrderMasuk=0; ptl.timeTombol=0; ptl.timeKonfirmMulai=0;
            xSemaphoreGive(ptlMutex);
            setLED(i, false, false);
            xSemaphoreTake(i2cMutex, portMAX_DELAY);
            oledStandby(i);
            xSemaphoreGive(i2cMutex);
            Serial.println("[CANCEL] PTL"+String(i+PTL_OFFSET)+" direset ke standby");
          } else {
            xSemaphoreGive(ptlMutex);
          }
        }
      }
    }
    return;
  }

  DynamicJsonDocument doc(512);
  if (deserializeJson(doc, msg)) { Serial.println("[MQTT] JSON parse gagal!"); return; }

  String orderId  = doc["order_id"].as<String>();
  String rackId   = doc["rack_id"].as<String>();
  String itemName = doc["item_name"].as<String>();
  int    qty      = doc["qty_to_put"].as<int>();
  int    ptlNum   = getRakPTL(rackId);

  if (ptlNum == 0) { Serial.println("[MQTT] Rak "+rackId+" skip (disabled/bukan ESP2)."); return; }

  if (xSemaphoreTake(ptlMutex, pdMS_TO_TICKS(100)) != pdTRUE) return;
  PTLState& ptl = getPTL(ptlNum);

  if (!ptl.aktif) {
    ptl.orderId        = orderId;
    ptl.itemName       = itemName;
    ptl.rackId         = rackId;
    ptl.qty            = qty;
    ptl.hasItem2       = false;
    ptl.aktif          = true;
    ptl.sedangKirim    = false;
    ptl.timeOrderMasuk = millis();
    Serial.println("[PTL"+String(ptlNum+PTL_OFFSET)+"] Item 1: "+orderId+" | "+itemName+" x"+String(qty));
  } else if (ptl.aktif && !ptl.hasItem2 && !ptl.sedangKirim) {
    ptl.orderId2  = orderId;
    ptl.itemName2 = itemName;
    ptl.rackId2   = rackId;
    ptl.qty2      = qty;
    ptl.hasItem2  = true;
    Serial.println("[PTL"+String(ptlNum+PTL_OFFSET)+"] Item 2: "+orderId+" | "+itemName+" x"+String(qty));
  } else {
    Serial.println("[MQTT] PTL"+String(ptlNum+PTL_OFFSET)+" penuh, skip.");
    xSemaphoreGive(ptlMutex);
    return;
  }

  xSemaphoreGive(ptlMutex);
  setLED(ptlNum, true, false);

  xSemaphoreTake(i2cMutex, portMAX_DELAY);
  PTLState& ptlOled = getPTL(ptlNum);
  oledOrder(ptlNum, ptlOled);
  xSemaphoreGive(i2cMutex);
}

void reconnectMQTT() {
  if (millis() - lastMqttRetry < 5000) return;
  lastMqttRetry = millis();
  Serial.println("[MQTT] Konek ke broker...");
  String clientId = "ESP2_PTL_" + String(random(0xffff), HEX);
  if (mqttClient.connect(clientId.c_str())) {
    Serial.println("[MQTT] Terhubung! Subscribe: " + String(TOPIC_ORDER) + " & " + String(TOPIC_CANCEL));
    mqttClient.subscribe(TOPIC_ORDER);
    mqttClient.subscribe(TOPIC_CANCEL);   // ✅ BARU
    xSemaphoreTake(i2cMutex, portMAX_DELAY);
    for (int i=1; i<=4; i++) {
      if (i == 2) continue;   // ⛔ PTL6 DISABLED — skip OLED yang rusak
      oledStandby(i);
    }
    xSemaphoreGive(i2cMutex);
  } else {
    Serial.println("[MQTT] Gagal rc=" + String(mqttClient.state()));
  }
}

void tcaSelect(uint8_t channel) {
  if (channel > 7) return;
  Wire.beginTransmission(TCA_ADDR);
  Wire.write(1 << channel);
  Wire.endTransmission();
}

int getOledChannel(int ptlNum) {
  switch(ptlNum) {
    case 1: return OLED_CH_PTL1;
    case 2: return OLED_CH_PTL2; // disabled tapi tetap ada biar gak crash
    case 3: return OLED_CH_PTL3;
    case 4: return OLED_CH_PTL4;
    default: return 0;
  }
}

Adafruit_SSD1306& getDisplay(int ptlNum) {
  switch(ptlNum) {
    case 1: return d1;
    case 2: return d2;
    case 3: return d3;
    case 4: return d4;
    default: return d1;
  }
}

PTLState& getPTL(int ptlNum) {
  switch(ptlNum) {
    case 1: return ptl1; case 2: return ptl2;
    case 3: return ptl3; case 4: return ptl4;
    default: return ptl1;
  }
}

QueueHandle_t getQueue(int ptlNum) {
  switch(ptlNum) {
    case 1: return queue1; case 2: return queue2;
    case 3: return queue3; case 4: return queue4;
    default: return queue1;
  }
}

void setup() {
  Serial.begin(115200);
  delay(1000);

  // ── GPIO ──────────────────────────────────
  pinMode(PTL1_LED_RED, OUTPUT); pinMode(PTL1_LED_GREEN, OUTPUT);
  pinMode(PTL2_LED_RED, OUTPUT); pinMode(PTL2_LED_GREEN, OUTPUT);
  pinMode(PTL3_LED_RED, OUTPUT); pinMode(PTL3_LED_GREEN, OUTPUT);
  pinMode(PTL4_LED_RED, OUTPUT); pinMode(PTL4_LED_GREEN, OUTPUT);
  pinMode(RELAY_PIN, OUTPUT);
  digitalWrite(RELAY_PIN, LOW);   // ✅ standby = LOW (samakan dengan ESP1)
  allLedOff();

  pinMode(PTL1_BTN, INPUT_PULLUP);
  pinMode(PTL2_BTN, INPUT_PULLUP);
  pinMode(PTL3_BTN, INPUT_PULLUP);
  pinMode(PTL4_BTN, INPUT_PULLUP);

  attachInterrupt(digitalPinToInterrupt(PTL1_BTN), isr1, FALLING);
  // attachInterrupt(digitalPinToInterrupt(PTL2_BTN), isr2, FALLING);  // ⛔ PTL6 DISABLED
  attachInterrupt(digitalPinToInterrupt(PTL3_BTN), isr3, FALLING);
  attachInterrupt(digitalPinToInterrupt(PTL4_BTN), isr4, FALLING);

  // ── Queue & Mutex ─────────────────────────
  queue1    = xQueueCreate(1, sizeof(KonfirmasiTask));
  queue2    = xQueueCreate(1, sizeof(KonfirmasiTask)); // tetap dibuat biar gak crash
  queue3    = xQueueCreate(1, sizeof(KonfirmasiTask));
  queue4    = xQueueCreate(1, sizeof(KonfirmasiTask));
  i2cMutex  = xSemaphoreCreateMutex();
  ptlMutex  = xSemaphoreCreateMutex();
  httpMutex = xSemaphoreCreateCounting(1, 1);

  // ── I2C & OLED ────────────────────────────
  Wire.begin(21, 22);
  Wire.setClock(100000);
  delay(500);

  Serial.println("=== Init OLED ESP2 ===");

  tcaSelect(OLED_CH_PTL1); delay(10);
  if (!d1.begin(SSD1306_SWITCHCAPVCC, 0x3C)) { Serial.println("OLED PTL5 GAGAL!"); while(true); }
  d1.clearDisplay(); d1.display(); Serial.println("OLED PTL5 OK!");

  // ⛔ PTL6/HMI2 DISABLED — modul OLED rusak (LDO mati), skip init
  // Wire.end(); delay(200); Wire.begin(21,22); Wire.setClock(100000); delay(200);
  // tcaSelect(OLED_CH_PTL2); delay(10);
  // if (!d2.begin(SSD1306_SWITCHCAPVCC, 0x3C)) { Serial.println("OLED PTL6 GAGAL!"); while(true); }
  // d2.clearDisplay(); d2.display(); Serial.println("OLED PTL6 OK!");
  Serial.println("OLED PTL6 SKIP (disabled)");

  Wire.end(); delay(200); Wire.begin(21,22); Wire.setClock(100000); delay(200);
  tcaSelect(OLED_CH_PTL3); delay(10);
  if (!d3.begin(SSD1306_SWITCHCAPVCC, 0x3C)) { Serial.println("OLED PTL7 GAGAL!"); while(true); }
  d3.clearDisplay(); d3.display(); Serial.println("OLED PTL7 OK!");

  Wire.end(); delay(200); Wire.begin(21,22); Wire.setClock(100000); delay(200);
  tcaSelect(OLED_CH_PTL4); delay(10);
  if (!d4.begin(SSD1306_SWITCHCAPVCC, 0x3C)) { Serial.println("OLED PTL8 GAGAL!"); while(true); }
  d4.clearDisplay(); d4.display(); Serial.println("OLED PTL8 OK!");

  Serial.println("=== OLED Done ===");

  // ── Tampil init ───────────────────────────
  oledTampilkan(1, "Pick to Light", "PTL 5", "Konek WiFi...", "");
  // oledTampilkan(2, "Pick to Light", "PTL 6", "Konek WiFi...", "");   // ⛔ PTL6 DISABLED
  oledTampilkan(3, "Pick to Light", "PTL 7", "Konek WiFi...", "");
  oledTampilkan(4, "Pick to Light", "PTL 8", "Konek WiFi...", "");

  // ── WiFi ──────────────────────────────────
  WiFi.disconnect(true); delay(1000);
  WiFi.mode(WIFI_STA); delay(500);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  int retry = 0;
  while (WiFi.status() != WL_CONNECTED && retry < 40) {
    delay(500); Serial.print("."); retry++;
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\nWiFi OK! IP: " + WiFi.localIP().toString());
    oledTampilkan(1, "WiFi OK!", WiFi.localIP().toString().c_str(), "PTL 5 Siap!", "");
    // oledTampilkan(2, "WiFi OK!", WiFi.localIP().toString().c_str(), "PTL 6 Siap!", "");  // ⛔ PTL6 DISABLED
    oledTampilkan(3, "WiFi OK!", WiFi.localIP().toString().c_str(), "PTL 7 Siap!", "");
    oledTampilkan(4, "WiFi OK!", WiFi.localIP().toString().c_str(), "PTL 8 Siap!", "");
    delay(1500);
  } else {
    Serial.println("\nWiFi GAGAL!");
    oledTampilkan(1, "WiFi GAGAL!", "Restart ESP", "", "");
    // oledTampilkan(2, "WiFi GAGAL!", "Restart ESP", "", "");   // ⛔ PTL6 DISABLED
    oledTampilkan(3, "WiFi GAGAL!", "Restart ESP", "", "");
    oledTampilkan(4, "WiFi GAGAL!", "Restart ESP", "", "");
    while(true);
  }

  // ── MQTT ──────────────────────────────────
  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  mqttClient.setCallback(mqttCallback);
  mqttClient.setBufferSize(512);

  // ── Standby ───────────────────────────────
  oledStandby(1);
  // oledStandby(2);   // ⛔ PTL6 DISABLED
  oledStandby(3);
  oledStandby(4);

  // ── FreeRTOS Tasks ────────────────────────
  xTaskCreatePinnedToCore(taskPTL1, "Task1", 8192, NULL, 1, NULL, 0);
  // xTaskCreatePinnedToCore(taskPTL2, "Task2", 8192, NULL, 1, NULL, 0);  // ⛔ PTL6 DISABLED
  xTaskCreatePinnedToCore(taskPTL3, "Task3", 8192, NULL, 1, NULL, 0);
  xTaskCreatePinnedToCore(taskPTL4, "Task4", 8192, NULL, 1, NULL, 0);

  Serial.println("=== ESP2 SIAP — PTL 5, 7, 8 AKTIF | PTL 6 DISABLED ===");
}

void loop() {
  if (!mqttClient.connected()) reconnectMQTT();
  mqttClient.loop();
  unsigned long now = millis();
  if (flag1 && now-lastDb1>DEBOUNCE_DELAY) { flag1=false; lastDb1=now; handleTombol(1); }
  // if (flag2 && now-lastDb2>DEBOUNCE_DELAY) { flag2=false; lastDb2=now; handleTombol(2); }  // ⛔ PTL6 DISABLED
  if (flag3 && now-lastDb3>DEBOUNCE_DELAY) { flag3=false; lastDb3=now; handleTombol(3); }
  if (flag4 && now-lastDb4>DEBOUNCE_DELAY) { flag4=false; lastDb4=now; handleTombol(4); }
}

void handleTombol(int ptlNum) {
  xSemaphoreTake(ptlMutex, portMAX_DELAY);
  PTLState& ptl = getPTL(ptlNum);
  if (!ptl.aktif || ptl.sedangKirim) { xSemaphoreGive(ptlMutex); return; }
  ptl.timeTombol       = millis();
  ptl.timeKonfirmMulai = millis();
  ptl.sedangKirim      = true;
  setLED(ptlNum, false, true);
  KonfirmasiTask task;
  task.ptlNum           = ptlNum;
  task.orderId          = ptl.orderId;
  task.orderId2         = ptl.orderId2;
  task.hasItem2         = ptl.hasItem2;
  task.respond_du       = (ptl.timeTombol - ptl.timeOrderMasuk) / 1000.0;
  task.timeOrderMasuk   = ptl.timeOrderMasuk;
  task.timeKonfirmMulai = ptl.timeKonfirmMulai;
  xSemaphoreGive(ptlMutex);
  xQueueSend(getQueue(ptlNum), &task, 0);
  xSemaphoreTake(i2cMutex, portMAX_DELAY);
  oledTampilkan(ptlNum, "Mengirim...", task.orderId.c_str(),
                ("PTL "+String(ptlNum+PTL_OFFSET)).c_str(), "Harap tunggu");
  xSemaphoreGive(i2cMutex);
}

void prosesKonfirmasi(KonfirmasiTask& task) {
  int ptlNum = task.ptlNum;
  xSemaphoreTake(httpMutex, portMAX_DELAY);

  bool confirmOK = false;
  int confirmRetry = 0;
  unsigned long timeSelesai = 0;

  while (!confirmOK && confirmRetry < 3) {
    HTTPClient http;
    String url = API_BASE + "/api/order/confirm"
               + "?order_id=" + task.orderId
               + "&respond_du=" + String(task.respond_du, 3);
    http.begin(url); http.setTimeout(8000);
    int code = http.GET(); http.end();
    if (code == 200) { confirmOK = true; timeSelesai = millis(); }
    else { confirmRetry++; delay(1000); }
  }

  if (task.hasItem2 && confirmOK) {
    bool confirm2OK = false; int retry2 = 0;
    while (!confirm2OK && retry2 < 3) {
      HTTPClient http3;
      String url3 = API_BASE + "/api/order/confirm"
                  + "?order_id=" + task.orderId2
                  + "&respond_du=" + String(task.respond_du, 3);
      http3.begin(url3); http3.setTimeout(8000);
      int code3 = http3.GET(); http3.end();
      if (code3 == 200) confirm2OK = true;
      else { retry2++; delay(1000); }
    }
  }

  xSemaphoreGive(httpMutex);

  xSemaphoreTake(ptlMutex, portMAX_DELAY);
  PTLState& ptl = getPTL(ptlNum);
  ptl.orderId=""; ptl.itemName=""; ptl.rackId=""; ptl.qty=0;
  ptl.orderId2=""; ptl.itemName2=""; ptl.rackId2=""; ptl.qty2=0;
  ptl.hasItem2=false; ptl.aktif=false; ptl.sedangKirim=false;
  ptl.timeOrderMasuk=0; ptl.timeTombol=0; ptl.timeKonfirmMulai=0;
  xSemaphoreGive(ptlMutex);

  if (!confirmOK) {
    setLED(ptlNum, false, false);
    xSemaphoreTake(i2cMutex, portMAX_DELAY);
    oledTampilkan(ptlNum, "GAGAL!", "Konfirmasi", "tidak terkirim", "Coba order lagi");
    xSemaphoreGive(i2cMutex);
    delay(2000);
    xSemaphoreTake(i2cMutex, portMAX_DELAY);
    oledStandby(ptlNum);
    xSemaphoreGive(i2cMutex);
    return;
  }

  float confirm_du = (timeSelesai - task.timeKonfirmMulai) / 1000.0;
  float total_dur  = (timeSelesai - task.timeOrderMasuk) / 1000.0;

  xSemaphoreTake(httpMutex, portMAX_DELAY);
  HTTPClient http2;
  String url2 = API_BASE + "/api/log/update"
              + "?order_id=" + task.orderId
              + "&confirm_du=" + String(confirm_du, 3)
              + "&total_dur="  + String(total_dur, 3);
  http2.begin(url2); http2.setTimeout(5000);
  http2.GET(); http2.end();
  xSemaphoreGive(httpMutex);

  xSemaphoreTake(i2cMutex, portMAX_DELAY);
  oledSukses(ptlNum);
  xSemaphoreGive(i2cMutex);
  delay(1500);
  setLED(ptlNum, false, false);
  delay(500);
  xSemaphoreTake(i2cMutex, portMAX_DELAY);
  oledStandby(ptlNum);
  xSemaphoreGive(i2cMutex);

  Serial.println("[TASK"+String(ptlNum+PTL_OFFSET)+"] Selesai! dur:"+String(total_dur,1)+"s");
}

void taskPTL1(void* p) { KonfirmasiTask t; while(true) { if(xQueueReceive(queue1,&t,portMAX_DELAY)) prosesKonfirmasi(t); } }
void taskPTL2(void* p) { KonfirmasiTask t; while(true) { if(xQueueReceive(queue2,&t,portMAX_DELAY)) prosesKonfirmasi(t); } }
void taskPTL3(void* p) { KonfirmasiTask t; while(true) { if(xQueueReceive(queue3,&t,portMAX_DELAY)) prosesKonfirmasi(t); } }
void taskPTL4(void* p) { KonfirmasiTask t; while(true) { if(xQueueReceive(queue4,&t,portMAX_DELAY)) prosesKonfirmasi(t); } }

void setLED(int ptlNum, bool red, bool green) {
  switch(ptlNum) {
    case 1: digitalWrite(PTL1_LED_RED,red?HIGH:LOW); digitalWrite(PTL1_LED_GREEN,green?HIGH:LOW); break;
    case 2: digitalWrite(PTL2_LED_RED,red?HIGH:LOW); digitalWrite(PTL2_LED_GREEN,green?HIGH:LOW); break;
    case 3: digitalWrite(PTL3_LED_RED,red?HIGH:LOW); digitalWrite(PTL3_LED_GREEN,green?HIGH:LOW); break;
    case 4: digitalWrite(PTL4_LED_RED,red?HIGH:LOW); digitalWrite(PTL4_LED_GREEN,green?HIGH:LOW); break;
  }
  updateRelay();
}

void updateRelay() {
  bool adaAktif = ptl1.aktif || ptl2.aktif || ptl3.aktif || ptl4.aktif;
  digitalWrite(RELAY_PIN, adaAktif ? HIGH : LOW);   // ✅ FIX: samakan dengan ESP1
}

void allLedOff() {
  digitalWrite(PTL1_LED_RED,LOW); digitalWrite(PTL1_LED_GREEN,LOW);
  digitalWrite(PTL2_LED_RED,LOW); digitalWrite(PTL2_LED_GREEN,LOW);
  digitalWrite(PTL3_LED_RED,LOW); digitalWrite(PTL3_LED_GREEN,LOW);
  digitalWrite(PTL4_LED_RED,LOW); digitalWrite(PTL4_LED_GREEN,LOW);
  digitalWrite(RELAY_PIN, LOW);   // ✅ FIX: standby = LOW (samakan dengan ESP1)
}

void oledStandby(int ptlNum) {
  tcaSelect(getOledChannel(ptlNum));
  Adafruit_SSD1306& d = getDisplay(ptlNum);
  d.clearDisplay(); d.setTextColor(SSD1306_WHITE);
  d.setTextSize(2); d.setCursor(25, 5);
  d.println("PTL " + String(ptlNum + PTL_OFFSET));
  String rak1, rak2;
  switch(ptlNum) {
    case 1: rak1="C-01"; rak2="C-02"; break;
    case 2: rak1="C-03"; rak2="C-04"; break;
    case 3: rak1="D-01"; rak2="D-02"; break;
    case 4: rak1="D-03"; rak2="D-04"; break;
  }
  d.setCursor(5, 25); d.print(rak1); d.print(" "); d.println(rak2);
  d.setTextSize(1); d.setCursor(35, 50); d.println("STANDBY");
  d.display();
}

void oledOrder1Item(int ptlNum, PTLState& ptl) {
  tcaSelect(getOledChannel(ptlNum));
  Adafruit_SSD1306& d = getDisplay(ptlNum);
  d.clearDisplay(); d.setTextColor(SSD1306_WHITE);
  d.setTextSize(1); d.setCursor(0, 0);
  d.println("PTL " + String(ptlNum + PTL_OFFSET) + " | " + ptl.rackId);
  d.setCursor(0, 14); d.println(ptl.itemName);
  d.setTextSize(2); d.setCursor(10, 32);
  d.print(ptl.qty); d.print(" pcs");
  d.setTextSize(1); d.setCursor(0, 55);
  d.println("[T" + String(ptlNum) + "] KONFIRMASI");
  d.display();
}

void oledOrder2Item(int ptlNum, PTLState& ptl) {
  tcaSelect(getOledChannel(ptlNum));
  Adafruit_SSD1306& d = getDisplay(ptlNum);
  d.clearDisplay(); d.setTextColor(SSD1306_WHITE); d.setTextSize(1);
  d.setCursor(0, 0); d.println("PTL " + String(ptlNum + PTL_OFFSET) + " | 2 ITEM");
  String nama1 = ptl.itemName;
  if (nama1.length() > 21) nama1 = nama1.substring(0, 20) + ".";
  d.setCursor(0, 14); d.println("1." + nama1);
  d.setCursor(0, 26); d.println("  " + ptl.rackId + " | " + String(ptl.qty) + " pcs");
  String nama2 = ptl.itemName2;
  if (nama2.length() > 21) nama2 = nama2.substring(0, 20) + ".";
  d.setCursor(0, 40); d.println("2." + nama2);
  d.setCursor(0, 52); d.println("  " + ptl.rackId2 + " | " + String(ptl.qty2) + " pcs");
  d.display();
}

void oledOrder(int ptlNum, PTLState& ptl) {
  if (!ptl.hasItem2) oledOrder1Item(ptlNum, ptl);
  else oledOrder2Item(ptlNum, ptl);
}

void oledSukses(int ptlNum) {
  tcaSelect(getOledChannel(ptlNum));
  Adafruit_SSD1306& d = getDisplay(ptlNum);
  d.clearDisplay(); d.setTextColor(SSD1306_WHITE);
  d.setTextSize(2); d.setCursor(15,5); d.println("SUKSES!");
  d.drawLine(0,26,127,26,SSD1306_WHITE);
  d.setTextSize(1);
  d.setCursor(0,32); d.println("PTL "+String(ptlNum+PTL_OFFSET)+" selesai!");
  d.setCursor(0,44); d.println("Konfirmasi terkirim");
  d.setCursor(0,54); d.println("Stok diperbarui");
  d.display();
}

void oledTampilkan(int ptlNum, const char* b1, const char* b2, const char* b3, const char* b4) {
  tcaSelect(getOledChannel(ptlNum));
  Adafruit_SSD1306& d = getDisplay(ptlNum);
  d.clearDisplay(); d.setTextColor(SSD1306_WHITE); d.setTextSize(1);
  d.setCursor(0,0);  d.println(b1);
  d.setCursor(0,16); d.println(b2);
  d.setCursor(0,32); d.println(b3);
  d.setCursor(0,48); d.println(b4);
  d.display();
}