// backend/wa-server.js
import crypto from "crypto";
import "dotenv/config";
import express from "express";
import cors from "cors";
import QRCode from "qrcode";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import pkg from "whatsapp-web.js";
const { Client, LocalAuth } = pkg;

/* ===== OCR & parsers ===== */
import * as TesseractNS from "tesseract.js";
const Tesseract = TesseractNS.default || TesseractNS;

import * as pdfParseCjs from "pdf-parse";
const pdfParse = pdfParseCjs.default || pdfParseCjs;

// sharp opcional (mejora OCR en imágenes)
let sharp = null;
try {
  const mod = await import("sharp");
  sharp = mod.default || mod;
} catch { /* opcional */ }

/* =========================
   Config
   ========================= */
const PORT = process.env.SERVER_PORT || 4000;
const HOST = process.env.SERVER_HOST || "0.0.0.0";

const SUPABASE_URL = (process.env.SUPABASE_URL || "").trim();
const SUPABASE_SERVICE_ROLE = (process.env.SUPABASE_SERVICE_ROLE || "").trim();

const PUPPETEER_EXECUTABLE_PATH =
  process.env.PUPPETEER_EXECUTABLE_PATH || undefined;
const WWEBJS_DATA_PATH =
  process.env.WWEBJS_DATA_PATH || path.join(process.cwd(), ".wwebjs_auth");

// bucket para comprobantes
const RECEIPTS_BUCKET = process.env.RECEIPTS_BUCKET || "receipts";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error("❌ Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE en .env");
  process.exit(1);
}

/* =========================
   Resiliencia procesos
   ========================= */
process.on("unhandledRejection", (reason) => {
  console.error("[GLOBAL] UnhandledRejection:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("[GLOBAL] UncaughtException:", err);
});

/* =========================
   Clients / DB
   ========================= */
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// --- Verificador de configuración Supabase (ref + prueba de consulta) ---
function b64urlDecode(str) {
  str = String(str).replace(/-/g, "+").replace(/_/g, "/");
  while (str.length % 4) str += "=";
  return Buffer.from(str, "base64").toString("utf8");
}
function decodeJwtPayload(jwt) {
  try {
    const [, p] = String(jwt).split(".");
    return p ? JSON.parse(b64urlDecode(p)) : null;
  } catch { return null; }
}

async function assertSupabaseConfig() {
  const urlRef =
    (SUPABASE_URL.match(/https?:\/\/([a-z0-9]{20})\.supabase\.co/i) || [null, null])[1];

  const payload = decodeJwtPayload(SUPABASE_SERVICE_ROLE);
  const claimRef = payload?.ref;
  const claimRole =
    payload?.role || payload?.roles || payload?.app_metadata?.role || undefined;

  if (!urlRef) {
    console.error("❌ URL de Supabase inválida:", SUPABASE_URL);
    process.exit(1);
  }
  if (!payload) {
    console.warn("⚠️ No pude decodificar el JWT de SERVICE_ROLE (seguimos y probamos conexión).");
  } else if (claimRef && claimRef !== urlRef) {
    console.error("❌ La SERVICE_ROLE corresponde a otro proyecto.", { claimRef, urlRef });
    process.exit(1);
  } else if (claimRole && String(claimRole).toLowerCase() !== "service_role") {
    console.warn("⚠️ El claim 'role' no es 'service_role' (puede no venir en algunas claves). role=", claimRole);
  }

  // Prueba real de conexión
  const { error } = await supabase.from("lines").select("id").limit(1);
  if (error) {
    console.error("❌ Supabase no aceptó la clave SERVICE_ROLE:", error);
    process.exit(1);
  }
  console.log("✅ Supabase OK (conexión y permisos válidos).");
}
await assertSupabaseConfig();

/* ===== Meta CAPI helper (envío server-side) ===== */
async function sendMetaCapiEvent({
  page_id,
  event_name,        // 'PageView' | 'Lead' | 'Purchase' | ...
  value = undefined, // number (para Purchase)
  currency = "ARS",
  external_id = null, // teléfono/email en claro (se hashea acá)
  event_source_url = null,
  action_source = "chat", // 'chat' porque el origen es WhatsApp
}) {
  try {
    if (!page_id) return;

    // Traemos las credenciales (pixel/token) de esa landing
    const { data: page, error } = await supabase
      .from("pages")
      .select("fb_pixel_id, fb_access_token, fb_test_event_code")
      .eq("id", page_id)
      .maybeSingle();

    if (error || !page?.fb_pixel_id || !page?.fb_access_token) return;

    const now = Math.floor(Date.now() / 1000);
    const event_id = crypto.randomUUID();

    const user_data = {};
    if (external_id) {
      user_data.external_id = crypto
        .createHash("sha256")
        .update(String(external_id).trim().toLowerCase())
        .digest("hex");
    }

    const payload = {
      data: [
        {
          event_name,
          event_time: now,
          event_id,
          action_source,
          event_source_url: event_source_url || undefined,
          user_data,
          custom_data:
            event_name === "Purchase" && Number.isFinite(value)
              ? { value, currency }
              : {},
          test_event_code: page.fb_test_event_code || undefined,
        },
      ],
    };

    const resp = await fetch(
      `https://graph.facebook.com/v18.0/${page.fb_pixel_id}/events?access_token=${page.fb_access_token}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }
    );

    if (!resp.ok) {
      const j = await resp.json().catch(() => ({}));
      console.warn("Meta CAPI error:", j);
    }
  } catch (e) {
    console.warn("sendMetaCapiEvent failed:", e?.message || e);
  }
}

await assertSupabaseConfig();

const lines = new Map(); // line_id -> { client, status, lastQrDataUrl, phone, project_id }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const seenMsgs = new Set(); // Dedupe de mensajes

/* =========================
   Helpers
   ========================= */
async function resolveMyNumber(client, { tries = 60, delay = 500 } = {}) {
  for (let i = 0; i < tries; i++) {
    try {
      const phone =
        client?.info?.wid?.user ||
        client?.info?.wid?._serialized?.split("@")[0] ||
        null;
      if (phone) return phone;
    } catch {}
    await sleep(delay);
  }
  return null;
}

function mapStatusForDb(status) {
  const map = {
    qr: "qr",
    ready: "connected",
    authenticated: "authenticated",
    disconnected: "disconnected",
    initializing: "initializing",
    loading: "loading",
    error: "error",
    restarting: "restarting",
  };
  return map[status] || status;
}

function clearLocalAuth(line_id) {
  const sessionDir = path.join(WWEBJS_DATA_PATH, `session-line-${line_id}`);
  try {
    fs.rmSync(sessionDir, { recursive: true, force: true });
    console.log(`[${line_id}] 🧹 Sesión local borrada: ${sessionDir}`);
  } catch (e) {
    console.warn(`[${line_id}] No se pudo borrar sesión local:`, e?.message || e);
  }
}

async function fetchProjectIdForLine(line_id) {
  const { data, error } = await supabase
    .from("lines")
    .select("project_id")
    .eq("id", line_id)
    .maybeSingle();

  if (error) {
    console.error("[lines] get project_id error:", error);
    return null;
  }
  return data?.project_id || null;
}

/** Upsert whatsapp_sessions con project_id garantizado (evita NOT NULL). */
async function upsertSessionRow(line_id, patch) {
  let project_id =
    patch.project_id ??
    lines.get(line_id)?.project_id ??
    (await fetchProjectIdForLine(line_id));

  if (!project_id) {
    console.warn(`[${line_id}] whatsapp_sessions omitido: project_id null`);
    return;
  }

  const payload = {
    line_id,
    project_id,
    updated_at: new Date().toISOString(),
    ...patch,
  };

  const { error } = await supabase
    .from("whatsapp_sessions")
    .upsert(payload, { onConflict: "line_id" });

  if (error) console.error("[whatsapp_sessions] upsert error:", error);
}

async function updateLinesRow(line_id, patch) {
  const dbPatch = {};
  if ("status" in patch) dbPatch.status = mapStatusForDb(patch.status);
  if ("phone" in patch) dbPatch.phone = patch.phone || null;

  if (Object.keys(dbPatch).length) {
    const { error } = await supabase
      .from("lines")
      .update(dbPatch)
      .eq("id", line_id);
    if (error) console.error("[lines] update error:", error);
  }
}

/** Guarda en memoria + DB (whatsapp_sessions y lines) */
async function setState(line_id, patch) {
  const st = lines.get(line_id) || {};
  const next = { ...st, ...patch };
  lines.set(line_id, next);

  const sess = {};
  if ("status" in patch) sess.wa_status = mapStatusForDb(patch.status);
  if ("phone" in patch) sess.wa_phone = patch.phone || null;
  if (next.project_id) sess.project_id = next.project_id;

  if (Object.keys(sess).length) {
    await upsertSessionRow(line_id, sess);
    await updateLinesRow(line_id, patch);
  }
  return next;
}

// === OCR: imagen/PDF -> texto ===============================================
async function ocrFromMedia({ base64, mimetype }) {
  try {
    const buf = Buffer.from(base64 || "", "base64");
    if (!buf.length) return "";

    // PDFs
    if (mimetype === "application/pdf" || /\.pdf$/i.test(mimetype || "")) {
      try {
        const { text } = await pdfParse(buf);
        return (text || "").toString();
      } catch (e) {
        console.warn("[OCR] pdf-parse error:", e?.message || e);
        return "";
      }
    }

    // Imágenes: pre-proceso suave si sharp está disponible
    let img = buf;
    if (sharp && /^image\/(jpe?g|png|webp)$/i.test(mimetype || "")) {
      try {
        img = await sharp(buf)
          .rotate() // endereza EXIF
          .resize({ width: 1600, withoutEnlargement: true })
          .grayscale()
          .normalize()
          .toFormat("png")
          .toBuffer();
      } catch (e) {
        console.warn("[OCR] sharp pipeline error:", e?.message || e);
      }
    }

    // Tesseract
    if (!Tesseract || typeof Tesseract.recognize !== "function") {
      console.warn("[OCR] Tesseract.recognize no disponible");
      return "";
    }

    const { data } = await Tesseract.recognize(img, "spa+eng", {
      tessedit_char_whitelist:
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz$.,:-/ ",
    });

    return data?.text || "";
  } catch (e) {
    console.warn("[OCR] error:", e?.message || e);
    return "";
  }
}

/* ======== Regex + parsers (Argentina) ======== */
const RE_CUIT  = /\b\d{2}-?\d{8}-?\d\b/g;
const RE_CBU   = /\b\d{22}\b/g;
const RE_ALIAS = /\b[a-z0-9._-]{6,}\b/gi;
const RE_REF   = /(referencia|ref\.?|c[oó]digo|cod\.?)\s*[:\-]?\s*([A-Z0-9\-]+)/gi;
const RE_TXN   = /(operaci[oó]n|transacci[oó]n|nro\.?\s*op\.?)\s*[:\-]?\s*([A-Z0-9\-]+)/gi;
const RE_BANK  =
  /(banco\s+[A-Za-zÁÉÍÓÚÑ .]+|mercado\s*pago|mercado\s*libre|uala|u?al[aá]|santander|galicia|macro|naci[óo]n|provincia|bbva|patagonia|credicoop|brubank|hsbc|icbc|naranja\s*x|prex)/gi;

/* ======== Bancos comunes (para detección automática) ======== */
const RE_BANK_NAMES = [
  { rx: /mercado\s*pago|mercado\s*libre/i, name: "Mercado Pago" },
  { rx: /\bual[aá]\b/i,                     name: "Ualá" },
  { rx: /\bsantander\b/i,                   name: "Santander" },
  { rx: /\bgalicia\b/i,                     name: "Galicia" },
  { rx: /\bbbva\b/i,                        name: "BBVA" },
  { rx: /\bmacro\b/i,                       name: "Macro" },
  { rx: /\bhsbc\b/i,                        name: "HSBC" },
  { rx: /\bicbc\b/i,                        name: "ICBC" },
  { rx: /\bnaci[óo]n\b|\bbna\b/i,           name: "Banco Nación" },
  { rx: /\bpatagonia\b/i,                   name: "Patagonia" },
  { rx: /\bcredicoop\b/i,                   name: "Credicoop" },
  { rx: /\bbrubank\b/i,                     name: "Brubank" },
  { rx: /\bnaranja\s*x\b/i,                 name: "Naranja X" },
  { rx: /\bprex\b/i,                        name: "Prex" },
];

function guessBank(text = "") {
  for (const b of RE_BANK_NAMES) {
    if (b.rx.test(text)) return b.name;
  }
  return null;
}
/// === OCR extra: fallback agresivo por grilla (Mercado Pago) — v3 con triple-cero por tile ===
async function tryExtractAmountFromImage({ base64, mimetype }) {
  if (!sharp) return null;
  if (!/^image\/(jpe?g|png|webp)$/i.test(mimetype || "")) return null;

  const buf = Buffer.from(base64 || "", "base64");
  let W = 1200, H = 1800;
  try {
    const meta = await sharp(buf).metadata();
    W = Math.max(1, meta.width  || W);
    H = Math.max(1, meta.height || H);
  } catch {}

  // Zona típica de MP (header izq.)
  const X0 = 0.04, X1 = 0.70; // 4% → 70% del ancho
  const Y0 = 0.08, Y1 = 0.48; // 8% → 48% del alto

  // Grilla
  const COLS = 4;
  const ROWS = 6;

  const startX = Math.floor(W * X0);
  const startY = Math.floor(H * Y0);
  const spanW  = Math.max(1, Math.floor(W * (X1 - X0)));
  const spanH  = Math.max(1, Math.floor(H * (Y1 - Y0)));

  const tileW  = Math.max(1, Math.floor(spanW / COLS));
  const tileH  = Math.max(1, Math.floor(spanH / ROWS));

  const padW = Math.floor(W * 0.08);
  const padH = Math.floor(H * 0.04);

  const NBSP = "\u00A0", NNSP = "\u202F";
  const RE_$AMT   = new RegExp(String.raw`\$\s*([0-9][0-9.,\s${NBSP}${NNSP}]*)`);
  const RE_GROUP  = new RegExp(String.raw`\b([1-9][0-9]{0,2}(?:[.\s${NBSP}${NNSP}][0-9]{3})+|[1-9][0-9]{4,})(?:[.,]\d{1,2})?\b`);
  const RE_TRIPLE_ZERO_HINT = /[.,](?:0{3}|0{2}[oO]|0[oO]0|[oO]0{2})(?!\d)/;

  const pipelines = [
    i => i.grayscale().normalize().linear(1.35, -18),
    i => i.grayscale().normalize().median(1).linear(1.5, -20).threshold(150),
    i => i.grayscale().normalize().linear(1.8, -25).gamma(0.9),
  ];

  const readPiece = async (input) => {
    for (const psm of [6, 7]) {
      try {
        const { data } = await Tesseract.recognize(input, "spa+eng", {
          tessedit_char_whitelist: "0123456789$., ",
          tessedit_pageseg_mode: String(psm),
          preserve_interword_spaces: "1",
        });
        const raw = (data?.text || "").trim();
        if (!raw) continue;

        const hasTripleZero = RE_TRIPLE_ZERO_HINT.test(raw);

        let m = raw.match(RE_$AMT);
        if (m) {
          let v = toNumberARS(m[1]);
          if (Number.isFinite(v) && v < 1000 && hasTripleZero) v *= 1000;
          if (Number.isFinite(v) && v > 0) return v;
        }

        m = raw.match(RE_GROUP);
        if (m) {
          let v = toNumberARS(m[0]);
          if (Number.isFinite(v) && v < 1000 && hasTripleZero) v *= 1000;
          if (Number.isFinite(v) && v > 0) return v;
        }
      } catch {}
    }
    return null;
  };

  let best = null;

  for (let r = 0; r < ROWS && !best; r++) {
    for (let c = 0; c < COLS && !best; c++) {
      const baseLeft = startX + c * tileW;
      const baseTop  = startY + r * tileH;

      const left = Math.max(0, baseLeft - Math.floor(padW / 2));
      const top  = Math.max(0, baseTop  - Math.floor(padH / 2));

      let width  = Math.min(tileW + padW, W - left);
      let height = Math.min(tileH + padH, H - top);

      if (!Number.isFinite(width) || !Number.isFinite(height)) continue;
      width  = Math.floor(width);
      height = Math.floor(height);
      if (width <= 16 || height <= 16) continue;

      try {
        const resized = await sharp(buf)
          .extract({ left, top, width, height })
          .resize({ width: Math.max(600, width * 2), withoutEnlargement: false })
          .toBuffer();

        for (const make of pipelines) {
          const png = await make(sharp(resized).clone()).toFormat("png").toBuffer();
          const v = await readPiece(png);
          if (Number.isFinite(v) && v > 0) {
            best = best ? Math.max(best, v) : v;
            break;
          }
        }
      } catch {}
    }
  }
  return best;
}

// === OCR: imagen/PDF -> texto (duplicado legacy, no usado) ===================
async function ocrsaFromMedia({ base64, mimetype }) {
  try {
    const buf = Buffer.from(base64 || "", "base64");
    if (!buf.length) return "";

    if (mimetype === "application/pdf" || /\.pdf$/i.test(mimetype || "")) {
      try {
        const { text } = await pdfParse(buf);
        return (text || "").toString();
      } catch (e) {
        console.warn("[OCR] pdf-parse error:", e?.message || e);
        return "";
      }
    }

    let img = buf;
    if (sharp && /^image\/(jpe?g|png|webp)$/i.test(mimetype || "")) {
      try {
        img = await sharp(buf)
          .rotate()
          .resize({ width: 1600, withoutEnlargement: true })
          .grayscale()
          .normalize()
          .toFormat("png")
          .toBuffer();
      } catch (e) {
        console.warn("[OCR] sharp pipeline error:", e?.message || e);
      }
    }

    if (!Tesseract || typeof Tesseract.recognize !== "function") {
      console.warn("[OCR] Tesseract.recognize no disponible");
      return "";
    }

    const { data } = await Tesseract.recognize(img, "spa+eng", {
      tessedit_char_whitelist:
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz$.,:-/ ",
    });

    return data?.text || "";
  } catch (e) {
    console.warn("[OCR] error:", e?.message || e);
    return "";
  }
}

/* ======================
   Monto en ARS robusto (v2)
   ====================== */
const NBSP = "\u00A0";
const NNSP = "\u202F";
const RE_SEP = new RegExp(`[.\\s${NBSP}${NNSP}]`, "g");

function toNumberARS(raw) {
  if (raw == null) return null;

  const original = String(raw);

  let s = original
    .replace(/(?<=\d)[oO](?=\d)/g, "0")
    .replace(/[^\d.,\u00A0\u202F]/g, "")
    .replace(/\u00A0|\u202F/g, " ")
    .replace(/\s+/g, "")
    .replace(/^[.,]+|[.,]+$/g, "");

  if (!s) return null;

  const hasOcrTripleZero =
    /[.,](?:0{3}|0{2}[oO]|0[oO]0|[oO]0{2})(?!\d)/.test(original);

  if (s.includes(".") && s.includes(",")) {
    s = s.replace(/\./g, "").replace(",", ".");
    const v = parseFloat(s);
    return Number.isFinite(v) ? v : null;
  }

  if (s.includes(",")) {
    const looksThousandsComma = /^\d{1,3}(?:,\d{3})+(?:,\d{1,2})?$/.test(s);
    if (looksThousandsComma) {
      const parts = s.split(",");
      if (parts[parts.length - 1].length <= 2) {
        const dec = parts.pop();
        s = parts.join("") + "." + dec;
      } else {
        s = parts.join("");
      }
    } else {
      s = s.replace(/\./g, "").replace(",", ".");
    }
    const v = parseFloat(s);
    return Number.isFinite(v) ? v : null;
  }

  if (s.includes(".")) {
    const parts = s.split(".");
    const last = parts[parts.length - 1];

    if (hasOcrTripleZero) {
      s = s.replace(/\./g, "");
      const v = parseFloat(s);
      return Number.isFinite(v) ? v : null;
    }

    if (/^0{3}$/.test(last) || last.length === 3) {
      s = s.replace(/\./g, "");
      const v = parseFloat(s);
      return Number.isFinite(v) ? v : null;
    }

    if (/^\d{1,3}(?:\.\d{3})+(?:\.\d{1,2})?$/.test(s)) {
      const dec = parts.pop();
      s = parts.join("") + "." + dec;
      const v = parseFloat(s);
      return Number.isFinite(v) ? v : null;
    }

    let v = parseFloat(s);

    if (Number.isFinite(v) && v < 1000 && /\.0{3,}\b/.test(original)) {
      v *= 1000;
    }
    return Number.isFinite(v) ? v : null;
  }

  const v = parseFloat(s);
  return Number.isFinite(v) ? v : null;
}

// 🔎 Detector de monto muy tolerante
function findBestAmount(text = "") {
  if (!text) return null;

  const norm = String(text)
    .replace(/\r/g, "")
    .replace(/[‘’´`]/g, "'")
    .replace(/[“”]/g, '"')
    .replace(new RegExp(`[${NBSP}${NNSP}]`, "g"), " ")
    .replace(/S\s*\$/gi, "$")
    .replace(/\bS\s*([0-9])/gi, "$$1")
    .replace(/\bARS\s*/gi, "$");

  const lines = norm.split(/\n+/).map(s => s.trim()).filter(Boolean);

  const BAD_CTX =
    /(cuit|cuil|cvu|cbu|coelsa|operaci[oó]n|transacci[oó]n|identificaci[oó]n|c[oó]digo|n[uú]mero|referencia)/i;

  const KEY_NEAR =
    /(comprobante|transferencia|motivo|mercado\s*pago|pagaste|enviaste|de\b|para\b|monto|importe|total)/i;

  const toFloatFlexible = (raw) =>
    toNumberARS(String(raw).replace(new RegExp(`[${NBSP}${NNSP}]`, "g"), " ").trim());

  const candidates = [];
  const pushCand = (v, prio) => {
    if (Number.isFinite(v) && v >= 50 && v <= 10_000_000) candidates.push({ v, prio });
  };

  const RE_CURRENCY_ANY = /\$\s*([0-9][0-9.,\s\u00A0\u202F]*)/g;

  lines.forEach((line) => {
    if (!line || BAD_CTX.test(line) || !/\$/.test(line)) return;
    let m;
    while ((m = RE_CURRENCY_ANY.exec(line)) !== null) {
      const v = toFloatFlexible(m[1]);
      pushCand(v, 6);
    }
  });

  const RE_GROUPED_OR_LONG =
    /\b([1-9][0-9]{0,2}(?:[.,\s\u00A0\u202F][0-9]{3})+|[1-9][0-9]{4,})(?:[.,]\d{1,2})?\b/g;

  if (candidates.length === 0) {
    lines.forEach((line, idx) => {
      if (!line || BAD_CTX.test(line)) return;

      let m;
      while ((m = RE_GROUPED_OR_LONG.exec(line)) !== null) {
        const raw = m[0];
        if (!/[.,\s\u00A0\u202F]/.test(raw)) {
          const asInt = parseInt(raw, 10);
          if (asInt >= 1900 && asInt <= 2099) continue;
        }
        const v = toFloatFlexible(raw);

        let bonus = 0;
        for (let k = Math.max(0, idx - 3); k <= Math.min(lines.length - 1, idx + 3); k++) {
          if (KEY_NEAR.test(lines[k])) {
            const dist = Math.abs(k - idx);
            bonus = Math.max(bonus, 3 - dist);
          }
        }
        pushCand(v, 2 + bonus);
      }
    });
  }

  if (candidates.length === 0) return null;

  const hasBig = candidates.some(c => c.v >= 1000);
  const pool = hasBig ? candidates.filter(c => c.v >= 1000) : candidates;

  pool.sort((a, b) => (b.prio - a.prio) || (b.v - a.v));
  return pool[0].v ?? null;
}

/* =========================
   Ciclo de vida del cliente
   ========================= */
function buildClient(line_id) {
  return new Client({
    authStrategy: new LocalAuth({
      clientId: `line-${line_id}`,
      dataPath: WWEBJS_DATA_PATH,
    }),
    puppeteer: {
      headless: true,
      executablePath: PUPPETEER_EXECUTABLE_PATH,
      args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
    },
  });
}

async function attachClient(line_id, client) {
  const state = {
    client,
    status: "initializing",
    lastQrDataUrl: null,
    phone: null,
    project_id: null,
  };

  state.project_id = await fetchProjectIdForLine(line_id);
  lines.set(line_id, state);
  await setState(line_id, {
    status: "initializing",
    phone: null,
    lastQrDataUrl: null,
    project_id: state.project_id,
  });

  client.on("loading_screen", async () => {
    await setState(line_id, { status: "loading" });
  });

  client.on("qr", async (qr) => {
    try {
      const dataUrl = await QRCode.toDataURL(qr);
      state.lastQrDataUrl = dataUrl;
      await setState(line_id, { status: "qr", phone: null, lastQrDataUrl: dataUrl });
      console.log(`[${line_id}] QR listo ✅`);
    } catch (e) {
      console.error(`[${line_id}] QR error`, e);
    }
  });

  client.on("authenticated", async () => {
    await setState(line_id, { status: "authenticated" });
  });

  client.on("ready", async () => {
    const myPhone = await resolveMyNumber(client);
    await setState(line_id, { status: "ready", phone: myPhone || null });

    await upsertSessionRow(line_id, {
      project_id: state.project_id || null,
      wa_status: "connected",
      wa_phone: myPhone || null,
    });

    console.log(
      `[${line_id}] WhatsApp conectado ${myPhone ? "(" + myPhone + ")" : ""}`
    );
  });

  // === TRACKING DE CHATS ENTRANTES + COMPROBANTES ===
  async function recordIncomingChat(msg) {
    try {
      if (!msg || !msg.id || !msg.id._serialized) return;
      const key = msg.id._serialized;
      if (seenMsgs.has(key)) return;
      seenMsgs.add(key);

      const jid = (msg.from || "").toLowerCase();
      if (!jid.endsWith("@c.us")) return;
      if (msg.fromMe) return;

      console.log(`[${line_id}] msg:`, {
        type: msg.type,
        hasMedia: msg.hasMedia,
        mimetype: msg._data?.mimetype || null,
        filename: msg._data?.filename || null,
        bodyLen: (msg.body || "").length,
      });

      const contact = jid.split("@")[0].replace(/\D+/g, "");
      const body = msg.body || "";
      const caption = (msg.caption || "").trim();
      const textForTag = [body, caption].filter(Boolean).join(" ");

      const tagMatch = textForTag.match(/#p:([a-z0-9._-]+)/i);
      const taggedSlug = tagMatch ? tagMatch[1] : null;

      const st = lines.get(line_id) || {};
      let project_id = st.project_id || (await fetchProjectIdForLine(line_id));
      let page_id = null;
      let slug = taggedSlug || null;

      if (taggedSlug) {
        const { data: page } = await supabase
          .from("pages")
          .select("id, slug, project_id")
          .eq("slug", taggedSlug)
          .maybeSingle();
        if (page) {
          page_id = page.id;
          slug = page.slug;
          project_id = page.project_id || project_id;
        }
      }

      const wa_phone = st.phone || null;

      // 1) registrar chat
      const { error: insErr } = await supabase.from("analytics_chats").insert({
        project_id,
        page_id,
        slug,
        line_id,
        wa_phone,
        contact,
        message: body || caption || "",
        created_at: new Date().toISOString(),
      });
      if (insErr) console.error("[analytics_chats] insert error:", insErr);

      // 2) guardar nombre + avatar
      try {
        const cinfo = await msg.getContact();
        const name =
          cinfo?.pushname || cinfo?.name || cinfo?.shortName || cinfo?.verifiedName || null;
        let avatar_url = null;
        try { avatar_url = await cinfo.getProfilePicUrl(); } catch {}
        if (project_id && contact && (name || avatar_url)) {
          await supabase
            .from("wa_contact_names")
            .upsert(
              {
                project_id,
                phone: contact,
                name: name || null,
                avatar_url: avatar_url || null,
                updated_at: new Date().toISOString(),
              },
              { onConflict: "project_id,phone" }
            );
        }
      } catch (e) {
        console.warn(`[${line_id}] ⚠️ Error guardando contacto:`, e?.message || e);
      }

      // 3) agenda -> new
      const { error: agErr } = await supabase.from("agenda").upsert(
        {
          project_id,
          contact,
          wa_phone,
          source_slug: slug,
          source_page_id: page_id,
          last_message_at: new Date().toISOString(),
          status: "new",
          updated_at: new Date().toISOString(),
        },
        { onConflict: "project_id,contact" }
      );
      if (agErr) console.error("[agenda] upsert error:", agErr);

      // 3.1) LEAD ÚNICO (una vez por proyecto+teléfono)
try {
  const { data: leadExists, error: leadSelErr } = await supabase
    .from("analytics_leads")
    .select("id")
    .eq("project_id", project_id)
    .eq("contact", contact)
    .maybeSingle();

  if (!leadExists) {
    const { error: leadInsErr } = await supabase
      .from("analytics_leads")
      .insert({
        project_id,
        contact,
        wa_phone,
        source_slug: slug || null,
        source_page_id: page_id || null,
        created_at: new Date().toISOString(),
      });
    if (leadInsErr) console.error("[analytics_leads] insert error:", leadInsErr);
  }
} catch (e) {
  console.warn("[analytics_leads] upsert-once error:", e?.message || e);
}

     // === LEAD ÚNICO por número: “Hola mi codigo de descuento es…” ===
try {
  const text = (textForTag || "").toLowerCase();

  // Detecta variantes con/ sin acentos, con “:” o “-”, con texto extra
  const leadRe = /^\s*hola\s+mi\s+c[oó]digo\s+de\s+descuento\s+es\s*[:\-]?\s*\S+/i;

  if (leadRe.test(text)) {
    await supabase
      .from("analytics_leads")
      .upsert(
        {
          project_id,
          page_id,
          slug,
          contact,
          first_message: textForTag.slice(0, 1000), // guardamos el primero
          created_at: new Date().toISOString(),
        },
        { onConflict: "project_id,contact", ignoreDuplicates: false }
      );
  }
} catch (e) {
  console.warn("[leads] upsert error:", e?.message || e);
}

/* 🔵 Meta CAPI: LEAD (cuando llega un mensaje que califica y hay page_id)
   Requiere que ya exista la helper sendMetaCapiEvent() definida más arriba. */
try {
  if (page_id) {
    await sendMetaCapiEvent({
      page_id,
      event_name: "Lead",
      external_id: contact,      // teléfono del lead (el hash lo hace el endpoint)
      action_source: "chat"
    });
  }
} catch (e) {
  console.warn("[meta-capi] Lead send error:", e?.message || e);
}

      // 4) ¿es comprobante?
      const looksLikeMedia = msg.hasMedia === true || msg.type === "image" || msg.type === "document";
      if (looksLikeMedia) {
        let mediaObj = null;
        try {
          mediaObj = await msg.downloadMedia();
        } catch (e) {
          console.warn(`[${line_id}] ⚠️ downloadMedia error:`, e?.message || e);
        }

        if (mediaObj?.data) {
          const mimetype = mediaObj.mimetype || "";
          const isImage = /^image\/(jpeg|png|webp)$/i.test(mimetype);
          const isPdf   = mimetype === "application/pdf";

          if (isImage || isPdf) {
            const cap = (msg.caption || msg.body || "").trim();
            const ocrText = await ocrFromMedia({ base64: mediaObj.data, mimetype });
            const combined = [cap, ocrText].filter(Boolean).join("\n");

            let { score, amount, provider } = scoreReceiptText(combined);

            // === Reglas de normalización de monto ===
            const IS_MP = /mercado\s*pago/i.test(combined);

            // 1) patrón fuerte de miles -> usar el mayor (con filtros anti-CVU/CBU/CUIT)
if (!Number.isFinite(amount) || amount < 1000) {
  const NB = "\u00A0", NN = "\u202F";
  const RE_GROUPED =
    new RegExp(String.raw`\$?\s*([1-9]\d{0,2}(?:[.\s${NB}${NN}]\d{3})+)(?:[.,]\d{1,2})?\b`, "g");

  // mismas heurísticas que findBestAmount
  const BAD_CTX =
    /(cuit|cuil|cvu|cbu|coelsa|operaci[oó]n|transacci[oó]n|identificaci[oó]n|c[oó]digo|n[uú]mero|referencia)/i;
  const KEY_NEAR =
    /(comprobante|transferencia|motivo|mercado\s*pago|pagaste|de\b|para\b|monto|importe|total)/i;

  const linesForSafety = (combined || "")
    .replace(/\r/g, "")
    .split(/\n+/)
    .map(s => s.trim())
    .filter(Boolean);

  let maxBig = null;

  for (const ln of linesForSafety) {
    // descartar líneas sospechosas (CVU/CBU/CUIT/IDs)
    if (BAD_CTX.test(ln)) continue;

    const hasCurrency = /\$/.test(ln);
    const nearMoney   = KEY_NEAR.test(ln);

    // exigimos $ o keyword cercana en la MISMA línea
    if (!hasCurrency && !nearMoney) continue;

    let m;
    while ((m = RE_GROUPED.exec(ln)) !== null) {
      const raw = m[1];

      // descartar por longitud de dígitos (evita CVU/CBU/IDs)
      const digits = raw.replace(/[^\d]/g, "");
      const len = digits.length;
      if (len >= 15 || len === 22) continue; // 15+ dígitos (o 22 exactos) => no es monto

      const v = toNumberARS(raw);
      if (!Number.isFinite(v)) continue;

      // rango razonable
      if (v >= 1000 && v <= 10_000_000) {
        maxBig = maxBig ? Math.max(maxBig, v) : v;
      }
    }
  }

  if (Number.isFinite(maxBig)) {
    amount = maxBig;
    score = Math.max(score, 10);
    console.log(`[${line_id}] 🔧 Safety monto (mayor formateado con filtros) → $${amount}`);
  }
}

            // 2) pista ".000"/variantes -> escalar
            {
              const RE_TRIPLE_ZERO_HINT = /[.,](?:0{3}|0{2}[oO]|0[oO]0|[oO]0{2})(?!\d)/;
              if (Number.isFinite(amount) && amount < 1000 && RE_TRIPLE_ZERO_HINT.test(combined)) {
                amount = amount * 1000;
                score = Math.max(score, 10);
                console.log(`[${line_id}] 🔧 Ajuste miles por pista ".000" en texto → $${amount}`);
              }
            }

            // 3) regla MP x1000 via env
            const MP_FORCE_X1000 = (process.env.MP_FORCE_X1000 || "true") === "true";
            if (MP_FORCE_X1000 && IS_MP && Number.isFinite(amount) && amount > 0 && amount < 1000) {
              amount = amount * 1000;
              score = Math.max(score, 10);
              provider = provider || "Mercado Pago";
              console.log(`[${line_id}] ⚙️ Regla MP x1000 aplicada → $${amount}`);
            }

            // 4) Fallback visual (detección por grilla) sólo para MP
            if ((!Number.isFinite(amount) || amount <= 0) &&
                IS_MP &&
                /^image\/(jpeg|png|webp)$/i.test(mimetype)) {
              const fallbackAmount = await tryExtractAmountFromImage({ base64: mediaObj.data, mimetype });
              if (Number.isFinite(fallbackAmount) && fallbackAmount > 0) {
                amount = fallbackAmount;
                score = Math.max(score, 12);
                provider = provider || "Mercado Pago";
                console.log(`[${line_id}] 🔎 Fallback de monto OK → $${amount}`);
              } else {
                console.log(`[${line_id}] 🔎 Fallback de monto sin éxito`);
              }
            }

            // 5) volver a aplicar MP x1000 si el fallback quedó < 1000
            if (MP_FORCE_X1000 && IS_MP && Number.isFinite(amount) && amount > 0 && amount < 1000) {
              amount = amount * 1000;
              score = Math.max(score, 12);
              provider = provider || "Mercado Pago";
              console.log(`[${line_id}] ⚙️ Regla MP x1000 (post-fallback) → $${amount}`);
            }

            if (score >= 4 && amount && amount > 0) {
              await saveReceiptAndCreateConversion({
                project_id,
                page_id,
                slug,
                contact_phone: contact,
                wa_phone,
                media: mediaObj,
                captionText: combined,
                line_id,
                forceAmount: amount,
              });

              await supabase
                .from("agenda")
                .update({
                  status: "conversion",
                  last_message_at: new Date().toISOString(),
                  updated_at: new Date().toISOString(),
                })
                .eq("project_id", project_id)
                .eq("contact", contact);

              console.log(`[${line_id}] ✅ Comprobante procesado (score:${score}, $${amount})`);
             
    // 🔵 Meta CAPI: Purchase
  try {
    if (page_id && Number.isFinite(amount) && amount > 0) {
      await sendMetaCapiEvent({
        page_id,
        event_name: "Purchase",
        external_id: contact, // teléfono del cliente
        value: amount,
        currency: "ARS",
        action_source: "chat",
      });
    }
  } catch (e) {
    console.warn("[meta-capi] Purchase send error:", e?.message || e);
  }

            } else {
              console.log(`[${line_id}] ❎ Ignorado (no parece comprobante) score:${score}, amount:${amount}`);
            }
          }
        }
      }

      console.log(
        `[${line_id}] Chat registrado ✅ contact:${contact} | slug:${slug || "-"} | project:${project_id || "-"}`
      );
    } catch (err) {
      console.error(`[${line_id}] chat_track_error`, err);
    }
  }

  client.on("message", recordIncomingChat);
  client.on("message_create", recordIncomingChat);

  client.on("disconnected", async (reason) => {
    console.log(`[${line_id}] Desconectado:`, reason);
    await setState(line_id, {
      status: "disconnected",
      phone: null,
      lastQrDataUrl: null,
    });

    try { await client?.pupPage?.close().catch(() => {}); } catch {}
    try { await client?.pupBrowser?.close().catch(() => {}); } catch {}
    try { await client?.destroy(); } catch {}

    if (String(reason || "").toUpperCase().includes("LOGOUT")) {
      clearLocalAuth(line_id);
    }

    await upsertSessionRow(line_id, {
      project_id: state.project_id || null,
      wa_status: "disconnected",
      wa_phone: null,
    });

    lines.delete(line_id);
    await setState(line_id, { status: "restarting" });

    setTimeout(async () => {
      console.log(`[${line_id}] Reiniciando cliente...`);
      try {
        const fresh = buildClient(line_id);
        await attachClient(line_id, fresh);
        fresh.initialize().catch((e) =>
          console.error(`[${line_id}] init error tras restart`, e)
        );
      } catch (e) {
        console.error(`[${line_id}] restart fatal`, e);
        await setState(line_id, { status: "error" });
      }
    }, 1200);
  });

  return state;
}

async function ensureClient(line_id) {
  if (lines.has(line_id)) return lines.get(line_id);
  const client = buildClient(line_id);
  const state = await attachClient(line_id, client);
  client.initialize().catch((e) => console.error(`[${line_id}] init error`, e));
  return state;
}

/* =========================
   API HTTP
   ========================= */
const app = express();
app.use(cors());
app.use(express.json());

/** Página QR simple (dev) */
app.get("/qr", async (req, res) => {
  const { line_id } = req.query;
  if (!line_id) return res.status(400).send("line_id required");
  await ensureClient(line_id);

  res.send(`
  <html>
  <head>
    <meta charset="utf-8" />
    <title>Conectar WhatsApp</title>
    <style>
      body{min-height:100vh;display:grid;place-items:center;background:#0b0b0d;color:#eee;font-family:system-ui}
      .btn{margin-top:16px;padding:8px 14px;border-radius:10px;border:0;background:#222;color:#eee}
      img{border-radius:12px}
      .muted{color:#999}
    </style>
  </head>
  <body>
    <div id="app" style="text-align:center">
      <h2>Conectar WhatsApp</h2>
      <div id="qr-box">Generando QR…</div>
      <div class="muted" style="margin-top:10px">Línea: <b>${line_id}</b></div>
      <button class="btn" onclick="location.reload()">Refrescar</button>
    </div>
    <script>
      const qrBox = document.getElementById('qr-box');
      const es = new EventSource('/lines/${line_id}/events');
      es.onmessage = (ev) => {
        const d = JSON.parse(ev.data);
        if (d.status === 'ready') {
          qrBox.innerHTML = '✓ WhatsApp conectado (' + (d.phone || 'sin número') + ')';
          qrBox.style.color = '#9f9';
        } else if (['restarting','disconnected','initializing','loading','error'].includes(d.status)) {
          qrBox.innerHTML = (d.status === 'disconnected') ? 'Reconectando…' : 'Generando QR…';
          qrBox.style.color = '#ccc';
        } else if (d.qr) {
          qrBox.innerHTML = '<img src="' + d.qr + '" width="360" height="360" />';
          qrBox.style.color = '#eee';
        } else {
          qrBox.innerHTML = 'Generando QR…';
          qrBox.style.color = '#eee';
        }
      };
    </script>
  </body>
  </html>`);
});

/** SSE estado/QR */
app.get("/lines/:lineId/events", async (req, res) => {
  const { lineId } = req.params;
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const st = await ensureClient(lineId);
  res.write(
    `data: ${JSON.stringify({
      status: st.status,
      phone: st.phone || null,
      qr: st.lastQrDataUrl || null,
    })}\n\n`
  );

  let last = { s: st.status, q: st.lastQrDataUrl, p: st.phone };

  const timer = setInterval(() => {
    const cur = lines.get(lineId) || {};
    if (cur.status !== last.s || cur.lastQrDataUrl !== last.q || cur.phone !== last.p) {
      last = { s: cur.status, q: cur.lastQrDataUrl, p: cur.phone };
      res.write(
        `data: ${JSON.stringify({
          status: cur.status,
          phone: cur.phone || null,
          qr: cur.lastQrDataUrl || null,
        })}\n\n`
      );
    }
  }, 700);

  req.on("close", () => clearInterval(timer));
});

/** JSON con QR actual (si está listo) */
app.post("/lines/:lineId/qr", async (req, res) => {
  try {
    const { lineId } = req.params;
    const st = await ensureClient(lineId);

    if (st.status === "ready") {
      return res.json({ status: "ready", phone: st.phone, qr: null });
    }

    let tries = 0;
    while (!st.lastQrDataUrl && tries++ < 120) {
      await sleep(250);
    }
    return res.json({ status: st.status, qr: st.lastQrDataUrl || null });
  } catch (e) {
    console.error("qr_failed", e);
    res.status(500).json({ error: "qr_failed" });
  }
});

/** Estado simple */
app.get("/lines/:lineId/status", async (req, res) => {
  try {
    const { lineId } = req.params;
    const st = lines.get(lineId);
    if (!st) return res.json({ status: "not_initialized" });
    res.json({ status: st.status, phone: st.phone || null });
  } catch (e) {
    res.status(500).json({ error: "status_failed" });
  }
});

/** QR como PNG */
app.get("/lines/:lineId/qr.png", (req, res) => {
  const { lineId } = req.params;
  const st = lines.get(lineId);
  if (!st?.lastQrDataUrl) return res.status(404).send("QR no disponible");
  const base64 = st.lastQrDataUrl.split(",")[1];
  const buf = Buffer.from(base64, "base64");
  res.setHeader("Content-Type", "image/png");
  res.send(buf);
});

/** Reinicio manual */
app.post("/lines/:lineId/restart", async (req, res) => {
  const { lineId } = req.params;
  const st = lines.get(lineId);
  try {
    try { await st?.client?.pupPage?.close().catch(() => {}); } catch {}
    try { await st?.client?.pupBrowser?.close().catch(() => {}); } catch {}
    try { await st?.client?.destroy(); } catch {}
    lines.delete(lineId);
    clearLocalAuth(lineId);
    await setState(lineId, {
      status: "restarting",
      phone: null,
      lastQrDataUrl: null,
    });
    await ensureClient(lineId);
    res.json({ ok: true });
  } catch (e) {
    console.error("restart_failed", e);
    res.status(500).json({ error: "restart_failed" });
  }
});

/* =========================
   Rutas adicionales (API)
   ========================= */
import chatsApi from "./api/chats/new.js";
app.use("/", chatsApi);

/** Keep-alive */
setInterval(() => {}, 60 * 1000);

app.listen(PORT, HOST, () => {
  console.log(`💡 WA server escuchando en http://${HOST}:${PORT}`);
});

/* =========================
   Health-check periódico
   ========================= */
async function probeLine(line_id, st) {
  try {
    const clientState = await st?.client?.getState().catch(() => null);

    if (clientState === "CONNECTED") {
      await setState(line_id, { status: "ready" });
      await supabase
        .from("whatsapp_sessions")
        .update({ wa_status: "connected", updated_at: new Date().toISOString() })
        .eq("line_id", line_id);
      return;
    }

    await setState(line_id, { status: "disconnected", phone: null, lastQrDataUrl: null });
    await supabase
      .from("whatsapp_sessions")
      .update({ wa_status: "disconnected", updated_at: new Date().toISOString() })
      .eq("line_id", line_id);
  } catch {
    await setState(line_id, { status: "disconnected", phone: null, lastQrDataUrl: null });
    await supabase
      .from("whatsapp_sessions")
      .update({ wa_status: "disconnected", updated_at: new Date().toISOString() })
      .eq("line_id", line_id);
  }
}

setInterval(() => {
  for (const [line_id, st] of lines.entries()) {
    probeLine(line_id, st);
  }
}, 20_000);

// ===============================
// 🧩 Parser por PLANTILLA + extractReceiptFields + scoreReceiptText
// ===============================
function _normTextForTpl(s = "") {
  return (s || "")
    .replace(/\r/g, "")
    .replace(/\u00A0|\u202F/g, " ")
    .replace(/[‘’´`]/g, "'")
    .replace(/[“”]/g, '"')
    .replace(/S\s*\$/gi, "$")
    .replace(/\bS\s*([0-9])/gi, "$$1")
    .replace(/ARS\s*/gi, "$")
    .replace(/\s+/g, " ")
    .trim();
}

function _extractAmountFromLine(line = "") {
  const m = line.match(/(?:\$)\s*([0-9]{1,3}(?:[ .,\u00A0\u202F][0-9]{3})*(?:[.,][0-9]{1,2})?|[0-9]+(?:[.,][0-9]{1,2})?)/i);
  if (!m) return null;
  return toNumberARS(m[1]);
}

const TPLS = [
  {
    provider: "Mercado Pago",
    test: /mercado\s*pago|mercado\s*libre|c[oó]digo de identificaci[oó]n|comprobante de transferencia|pagaste/i,
    amountLine: /(?:pagaste|transferiste|monto|importe|total)\b/i,
  },
  {
    provider: "Naranja X",
    test: /naranja\s*x|enviaste/i,
    amountLine: /(?:enviaste|monto|importe|total)\b/i,
  },
  {
    provider: "Prex",
    test: /\bprex\b|comprobante de transferencia/i,
    amountLine: /(?:monto|importe|total|enviaste|transferiste)\b/i,
  },
  { provider: "Ualá", test: /ual[aá]\b|transferencia realizada|comprobante/i, amountLine: /(?:monto|importe|total|transferiste|transferencia)\b/i },
  { provider: "Banco Naci[oó]n|BNA", test: /banco\s+naci[oó]n|bna\b|bna\+/i, amountLine: /(?:monto|importe|total)\b/i },
  { provider: "Santander", test: /santander/i, amountLine: /(?:monto|importe|total)\b/i },
  { provider: "Galicia", test: /galicia/i, amountLine: /(?:monto|importe|total)\b/i },
];

function parseByTemplate(text = "") {
  if (!text) return { matched: false };

  const norm = _normTextForTpl(text);
  const lines = norm.split("\n").map(l => l.trim()).filter(Boolean);
  const all = norm;

  for (const tpl of TPLS) {
    if (!tpl.test.test(all)) continue;

    let best = null;
    for (const ln of lines) {
      if (tpl.amountLine.test(ln) || /\$/.test(ln)) {
        const v = _extractAmountFromLine(ln);
        if (Number.isFinite(v)) {
          if (!best || v > best) best = v;
        }
      }
    }

    if (!best) {
      const any = all.match(/(?:\$)\s*([0-9]{1,3}(?:[ .,\u00A0\u202F][0-9]{3})*(?:[.,][0-9]{1,2})?|[0-9]+(?:[.,][0-9]{1,2})?)/gi);
      if (any) {
        for (const hit of any) {
          const mm = hit.match(/(?:\$)\s*([0-9., \u00A0\u202F]+)/i);
          const v = mm ? toNumberARS(mm[1]) : null;
          if (Number.isFinite(v)) {
            if (!best || v > best) best = v;
          }
        }
      }
    }

    if (Number.isFinite(best) && best > 0) {
      const cuit = (all.match(/\b\d{2}-?\d{8}-?\d\b/) || [null])[0];
      const cvu  = (all.match(/\b\d{22}\b/) || [null])[0];

      return {
        matched: true,
        provider: tpl.provider,
        amount: best,
        fields: {
          cuit,
          cvu,
          nameFrom: (all.match(/\bde[:\s]+([A-ZÁÉÍÓÚÑa-záéíóúñ .]+)/i) || [null, null])[1]?.trim() || null,
          nameTo:   (all.match(/\bpara[:\s]+([A-ZÁÉÍÓÚÑa-záéíóúñ .]+)/i) || [null, null])[1]?.trim() || null,
        },
      };
    }
  }

  return { matched: false };
}

function extractReceiptFields(text = "") {
  const out = {
    amount: null,
    concept: null,
    transaction: null,
    reference: null,
    origin: { name: null, cuit: null, account: null, bank: null },
    destination: { name: null, cuit: null, account: null, bank: null },
  };
  if (!text) return out;

  const norm = _normTextForTpl(text);

  // ====== Monto (usa plantilla y fallback robusto) ======
  const tpl = parseByTemplate(text);
  out.amount = Number.isFinite(tpl.amount) ? tpl.amount : findBestAmount(text) || null;

  // ====== Concepto / Nº operación / Referencia ======
  const mTxn = norm.match(RE_TXN);
  if (mTxn) out.transaction = mTxn[2];

  const mRef = norm.match(RE_REF);
  if (mRef) out.reference = mRef[2];

  const mConcept = norm.match(/concepto\s*[:\-]?\s*(.+?)(?:\s{2,}|$)/i);
  if (mConcept) out.concept = mConcept[1].trim().slice(0, 120);

  // ====== Helpers de sección ======
  const sliceBetween = (s, startRx, endRx) => {
    const mStart = s.match(startRx);
    if (!mStart) return null;
    const from = mStart.index + mStart[0].length;
    const rest = s.slice(from);
    const mEnd = rest.match(endRx);
    return rest.slice(0, mEnd ? mEnd.index : rest.length).trim() || null;
  };

  const firstNiceName = (blk) => {
    if (!blk) return null;
    // "Nombre: X", "Titular: X", "Beneficiario: X"
    const tag = blk.match(/(?:nombre|titular|beneficiario)\s*[:\-]\s*([A-ZÁÉÍÓÚÑa-záéíóúñ .]+)/i);
    if (tag?.[1]) return tag[1].trim();
    // "De: X", "Para: X", "A: X"
    const dePara = blk.match(/\b(?:de|para|a)\s*[:\-]\s*([A-ZÁÉÍÓÚÑa-záéíóúñ .]+)/i);
    if (dePara?.[1]) return dePara[1].trim();
    // Primera línea con letras (sin muchos números)
    const first = blk
      .split(/\n+/)
      .map((l) => l.trim())
      .find((l) => /[A-Za-zÁÉÍÓÚÑáéíóúñ]/.test(l) && !/\d{5,}/.test(l));
    return first || null;
  };

  const firstMatch = (rx, s) => {
    const m = s?.match(rx);
    return m ? m[0] : null;
  };

  const pickAccount = (blk) => {
    if (!blk) return null;
    // CBU/CVU (22 dígitos)
    const cbu = firstMatch(RE_CBU, blk);
    if (cbu) return cbu;
    // Alias (si aparece con alguna etiqueta)
    const alias = blk.match(/(?:alias|cvu|cbu)\s*[:\-]?\s*([a-z0-9._-]{6,})/i);
    if (alias?.[1]) return alias[1].trim();
    // Último recurso: primer "alias" libre
    const loneAlias = blk.match(RE_ALIAS);
    if (loneAlias?.[0] && !/\d{10,}/.test(loneAlias[0])) return loneAlias[0];
    return null;
  };

  const pickCuit = (blk) => {
    const c = firstMatch(RE_CUIT, blk);
    return c ? c.replace(/-/g, "") : null;
  };

  const pickBank = (blk) => guessBank(blk) || firstMatch(RE_BANK, blk);

  // ====== Intento por bloques "Origen" / "Destino" ======
  const originBlk =
    sliceBetween(
      text,
      /(origen\b|^|\n)\s*(?:origen|de|desde|emisor|remitente)\s*[:\-]?\s*/i,
      /\b(destino|para|a\b|beneficiario|archivo|adjunto|comprobante)\b/i
    ) ||
    sliceBetween(
      text,
      /\bde\s*[:\-]?\s*/i,
      /\b(para|destino|archivo|adjunto|comprobante)\b/i
    );

  const destBlk =
    sliceBetween(
      text,
      /(destino\b|^|\n)\s*(?:destino|para|a|beneficiario|receptor)\s*[:\-]?\s*/i,
      /\b(archivo|adjunto|comprobante)\b/i
    ) ||
    sliceBetween(
      text,
      /\b(para|a)\s*[:\-]?\s*/i,
      /\b(archivo|adjunto|comprobante)\b/i
    );

  // Completar desde bloques si existen
  if (originBlk) {
    out.origin.name = firstNiceName(originBlk);
    out.origin.cuit = pickCuit(originBlk);
    out.origin.account = pickAccount(originBlk);
    out.origin.bank = pickBank(originBlk);
  }
  if (destBlk) {
    out.destination.name = firstNiceName(destBlk);
    out.destination.cuit = pickCuit(destBlk);
    out.destination.account = pickAccount(destBlk);
    out.destination.bank = pickBank(destBlk);
  }

  // ====== Fallback global si faltan datos ======
  // CUITs y cuentas en todo el texto (para repartir si no hubo bloques)
  const allCuist = [...norm.matchAll(RE_CUIT)].map((m) => m[0].replace(/-/g, ""));
  const allAccts = [...norm.matchAll(RE_CBU)].map((m) => m[0]);
  const allAlias = [...norm.matchAll(RE_ALIAS)].map((m) => m[0]).filter((a) => !/\d{10,}/.test(a));

  // Banco general si falta (ej: Mercado Pago)
  const bankGlobal = guessBank(text);

  // Heurística: si no tengo cuenta/cuit en origen/destino, asigno primero a origen y último a destino.
  if (!out.origin.cuit && allCuist.length) out.origin.cuit = allCuist[0];
  if (!out.destination.cuit && allCuist.length > 1) out.destination.cuit = allCuist[allCuist.length - 1];

  if (!out.origin.account && allAccts.length) out.origin.account = allAccts[0];
  if (!out.destination.account && allAccts.length > 1) out.destination.account = allAccts[allAccts.length - 1];

  // Si no hubo CBU/CVU, intento alias
  if (!out.origin.account && allAlias.length) out.origin.account = allAlias[0];
  if (!out.destination.account && allAlias.length > 1) out.destination.account = allAlias[allAlias.length - 1];

  // Banco por bloque o global
  if (!out.origin.bank) out.origin.bank = bankGlobal;
  if (!out.destination.bank) out.destination.bank = bankGlobal;

  // Nombre: si vino por plantilla, úsalo
  if (!out.origin.name && tpl?.fields?.nameFrom) out.origin.name = tpl.fields.nameFrom;
  if (!out.destination.name && tpl?.fields?.nameTo) out.destination.name = tpl.fields.nameTo;

  return out;
}

function scoreReceiptText(text = "") {
  const t = (text || "").replace(/\s+/g, " ").trim();
  let score = 0;

  // pequeños boosts por frases fuertes
  if (/comprobante\s+de\s+transferencia/i.test(t)) score += 2;
  if (/enviaste/i.test(t)) score += 1;

  const tpl = parseByTemplate(text);
  const amountTpl = tpl.matched ? tpl.amount : null;

  const hasComprobante = /comprobante/i.test(t);
  const hasTransfer = /transferencia/i.test(t);
  const hasMercadoPago = /mercado\s*pago/i.test(t);
  const hasKw = /pagaste|recibo|pago realizado|n[uú]mero de operaci[oó]n|c[oó]digo de identificaci[oó]n/i.test(t);
  const hasBank = /(mercado\s*pago|ual[aá]|santander|galicia|macro|bbva|hsbc|icbc|naci[oó]n|bna)/i.test(t);

  const amountHeur = findBestAmount(text);
  let amount = Number.isFinite(amountTpl) ? amountTpl : amountHeur;

  if (amount && amount < 1000) {
    const fallback = parseByTemplate(text);
    if (fallback.matched && fallback.amount > 1000) amount = fallback.amount;
  }

  const hasAmount = Number.isFinite(amount) && amount > 0;
  const hasId = /(operaci[oó]n|transacci[oó]n|c[oó]digo|identificaci[oó]n)\s*[:\-]?\s*[A-Z0-9\-]+/i.test(t);
  const parties = /(CUIT|CVU|CBU|\bcvu\b|\bcbu\b|beneficiario)/i.test(t);

  if (hasComprobante) score += 2;
  if (hasTransfer) score += 2;
  if (hasMercadoPago) score += 2;
  if (hasKw) score++;
  if (hasBank) score++;
  if (hasAmount) score += 3;
  if (hasId) score++;
  if (parties) score++;

  const hasCurrencySymbol = /\$/.test(t);
  const hasThousandsPattern = new RegExp(
    String.raw`\b[1-9]\d{0,2}(?:[.\s${NBSP}${NNSP}]\d{3})+(?:[,.\s]\d{1,2})?\b`
  ).test(t);

  if (hasCurrencySymbol) score += 1;
  if (hasThousandsPattern && Number.isFinite(amount) && amount >= 1000) score += 2;

  if (tpl.matched && hasAmount) score += 3;

  return {
    score,
    amount: hasAmount ? amount : null,
    provider: tpl.matched ? tpl.provider : null
  };
}

/** Scoring + creación de conversión */
async function saveReceiptAndCreateConversion({
  project_id,
  page_id,
  slug,
  contact_phone,
  wa_phone,
  media,
  captionText,
  line_id,
  forceAmount,
}) {
  let file_url = null;
  const file_mime = media?.mimetype || "application/octet-stream";
  const filename = media?.filename || "";
  const lower = (filename || "").toLowerCase();

  const isImage = /^image\//i.test(file_mime) || /\.(jpg|jpeg|png|webp|gif)$/i.test(lower);
  const isPdf   = file_mime === "application/pdf" || /\.pdf$/i.test(lower);

  if (media?.data && (isImage || isPdf)) {
    try {
      const ext =
        (isImage && (file_mime.split("/")[1] || lower.split(".").pop() || "jpg")) ||
        (isPdf && "pdf") || "bin";

      const ts        = Date.now();
      const safePhone = (contact_phone || "unknown").replace(/\D+/g, "");
      const safeProj  = (project_id || "na").toString();
      const filePath  = `${safeProj}/${safePhone}/${ts}.${ext}`;
      const buffer    = Buffer.from(media.data, "base64");

      const { error: upErr } = await supabase
        .storage
        .from(RECEIPTS_BUCKET)
        .upload(filePath, buffer, { contentType: file_mime, upsert: true });

      if (upErr) {
        console.error("[receipts] upload error:", upErr);
      } else {
        const { data: pub } = supabase.storage.from(RECEIPTS_BUCKET).getPublicUrl(filePath);
        file_url = pub?.publicUrl || null;
      }
    } catch (e) {
      console.warn("[receipts] upload exception:", e?.message || e);
    }
  }

  // 🔎 Parsear texto para extraer todos los campos
  const parsed = extractReceiptFields(captionText || "");
  const amount = (Number.isFinite(forceAmount) ? forceAmount : parsed.amount) ?? null;

  // Insertar TODO el detalle en analytics_conversions
  const payload = {
    project_id,
    page_id,
    slug,
    contact: contact_phone,
    wa_phone,

    file_url,
    file_mime,
    amount,
    status: "received",
    line_id,
    created_at: new Date().toISOString(),

    // detalle
    concept: parsed.concept || null,
    reference: parsed.reference || null,
    operation_no: parsed.transaction || null,

    origin_name: parsed.origin?.name || null,
    origin_cuit: parsed.origin?.cuit || null,
    origin_account: parsed.origin?.account || null,
    origin_bank: parsed.origin?.bank || null,

    dest_name: parsed.destination?.name || null,
    dest_cuit: parsed.destination?.cuit || null,
    dest_account: parsed.destination?.account || null,
    dest_bank: parsed.destination?.bank || null,
  };

  const { error: convErr } = await supabase
    .from("analytics_conversions")
    .insert(payload);

  if (convErr) console.error("[analytics_conversions] insert error:", convErr);

  // Mantener la agenda marcada como “conversion”
  const { error: agErr } = await supabase.from("agenda").upsert(
    {
      project_id,
      contact: contact_phone,
      wa_phone,
      source_slug: slug || null,
      source_page_id: page_id || null,
      status: "conversion",
      last_message_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
    { onConflict: "project_id,contact" }
  );
  if (agErr) console.error("[agenda] upsert conversion error:", agErr);

  return { file_url, amount };
}