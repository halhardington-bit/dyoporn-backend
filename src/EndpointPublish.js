import fs from "fs";
import path from "path";
import os from "os";
import crypto from "crypto";
import { spawn } from "child_process";
import multer from "multer";

import { getOrCreateUserMediaKey } from "./mediaKeys.js";

/* ============================================================
   UTIL
============================================================ */

function runCmd(cmd, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { windowsHide: true });

    let err = "";

    p.stderr.on("data", (d) => (err += d.toString()));

    p.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(err || `${cmd} failed`));
    });
  });
}

function parseCreationData(raw) {
  if (!raw) return {};
  try {
    return typeof raw === "object" ? raw : JSON.parse(raw);
  } catch {
    return {};
  }
}

function safeUnlink(p) {
  try {
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch {}
}

/* ============================================================
   CRYPTO
============================================================ */

// GCM unwrap for DEK
function unwrapDEK(encryptedDekB64, masterKey) {
  const payload = Buffer.from(encryptedDekB64, "base64");

  const iv = payload.subarray(0, 12);
  const tag = payload.subarray(12, 28);
  const data = payload.subarray(28);

  const decipher = crypto.createDecipheriv("aes-256-gcm", masterKey, iv);
  decipher.setAuthTag(tag);

  return Buffer.concat([decipher.update(data), decipher.final()]);
}

// CTR decrypt for media
function decryptCTR(inputPath, outputPath, key, ivB64) {
  const iv = Buffer.from(ivB64, "base64");

  return new Promise((resolve, reject) => {
    const decipher = crypto.createDecipheriv("aes-256-ctr", key, iv);

    const input = fs.createReadStream(inputPath);
    const output = fs.createWriteStream(outputPath);

    input.pipe(decipher).pipe(output);

    output.on("finish", resolve);
    output.on("error", reject);
  });
}

/* ============================================================
   MAIN ROUTE
============================================================ */

export function registerEndpointPublish(app, { pool, requireAuth }) {
  const upload = multer({ dest: os.tmpdir() });

  app.post(
    "/api/endpoint/publish",
    requireAuth,
    upload.single("media"),
    async (req, res) => {
      const userId = req.user.id;

      const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "publish-"));
      const decryptedPath = path.join(tmpDir, "output.mp4");

      const cleanup = () => {
        try {
          fs.rmSync(tmpDir, { recursive: true, force: true });
        } catch {}
        safeUnlink(req.file?.path);
      };

      try {
        /* ============================
           VALIDATION
        ============================ */

        if (!req.file?.path) {
          cleanup();
          return res.status(400).json({ error: "Missing media file" });
        }

        const creationData = parseCreationData(req.body.creationData);

        if (!creationData) {
          cleanup();
          return res.status(400).json({ error: "Missing creationData" });
        }

        const envelope =
          creationData.encryption_envelope ||
          creationData.encryptionEnvelope;

        if (!envelope) {
          cleanup();
          return res.status(400).json({
            error: "Missing encryption_envelope inside creationData",
          });
        }

        if (!envelope.encrypted_dek || !envelope.media_iv) {
          cleanup();
          return res.status(400).json({
            error: "Invalid encryption_envelope structure",
          });
        }

        /* ============================
           DECRYPTION
        ============================ */

        const rawKey = await getOrCreateUserMediaKey(userId);
        const userKey = Buffer.from(rawKey, "base64");

        if (userKey.length !== 32) {
          throw new Error(`Invalid key length: ${userKey.length}`);
        }

        const dek = unwrapDEK(envelope.encrypted_dek, userKey);

        await decryptCTR(
          req.file.path,
          decryptedPath,
          dek,
          envelope.media_iv
        );

        /* ============================
           FFMPEG (basic test)
        ============================ */

        await runCmd("ffmpeg", [
          "-y",
          "-i",
          decryptedPath,
          "-f",
          "null",
          "-"
        ]);

        /* ============================
           DB INSERT
        ============================ */

        const title = req.body.title || creationData.name || "Untitled";

        const result = await pool.query(
          `
          INSERT INTO videos (
            user_id,
            title,
            description,
            visibility,
            filename,
            creation_data,
            moderation_check
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7)
          RETURNING id
          `,
          [
            userId,
            title,
            req.body.description || "",
            req.body.visibility || "public",
            "placeholder.m3u8",
            JSON.stringify(creationData),
            false, // ALWAYS FALSE
          ]
        );

        cleanup();

        return res.json({
          ok: true,
          videoId: result.rows[0].id,
        });
      } catch (err) {
        console.error("❌ Publish error:", err);
        cleanup();
        return res.status(500).json({ error: err.message });
      }
    }
  );
}