// backend/api/chats/new.js
import express from "express";
import { createClient } from "@supabase/supabase-js";

const router = express.Router();

// Reutiliza variables de entorno del proceso
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

/**
 * POST /api/chats/new
 * Body JSON:
 * {
 *   project_id: uuid (requerido),
 *   page_id: uuid | null,
 *   slug: string | null,
 *   line_id: uuid | null,
 *   wa_phone: string | null,
 *   contact: string (tel del usuario, requerido),
 *   message: string | null,
 *   name: string | null   // opcional: guardar nombre del contacto
 * }
 */
router.post("/api/chats/new", async (req, res) => {
  try {
    const {
      project_id,
      page_id = null,
      slug = null,
      line_id = null,
      wa_phone = null,
      contact,
      message = null,
      name = null,
    } = req.body || {};

    if (!project_id || !contact) {
      return res.status(400).json({ ok: false, error: "project_id y contact son requeridos" });
    }

    // 1) Insertar registro de chat
    const { error: insErr } = await supabase.from("analytics_chats").insert({
      project_id,
      page_id,
      slug,
      line_id,
      wa_phone,
      contact,
      message,
    });
    if (insErr) {
      console.error("[analytics_chats] insert error:", insErr);
      return res.status(500).json({ ok: false, error: "insert_failed" });
    }

    // 2) Guardar nombre opcional
    if (name) {
      await supabase
        .from("wa_contact_names")
        .upsert(
          { project_id, phone: String(contact), name, updated_at: new Date().toISOString() },
          { onConflict: "project_id,phone" }
        );
    }

    // 3) Upsert en agenda (si usás esa tabla)
    await supabase.from("agenda").upsert(
      {
        project_id,
        contact: String(contact),
        wa_phone,
        source_slug: slug,
        source_page_id: page_id,
        last_message_at: new Date().toISOString(),
        status: "new",
        updated_at: new Date().toISOString(),
      },
      { onConflict: "project_id,contact" }
    );

    return res.json({ ok: true });
  } catch (e) {
    console.error("[api/chats/new] error:", e);
    return res.status(500).json({ ok: false, error: "server_error" });
  }
});

// ping opcional
router.get("/api/health", (_req, res) => res.json({ ok: true, service: "wa-server" }));

export default router;