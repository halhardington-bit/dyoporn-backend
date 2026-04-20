import express from "express";
import cors from "cors";
import morgan from "morgan";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import cookieParser from "cookie-parser";
import "dotenv/config";


import authRouter from "./auth.js";
import profileRouter from "./profile.js";
import { pool } from "./db.js";

import multer from "multer";
import crypto from "crypto";
import { spawn } from "child_process";
import os from "os";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

import { registerGeneratePublish } from "./generatePublish.js";
import { registerGenerateProjects } from "./generateProjects.js";
import { getOrCreateUserMediaKey } from "./mediaKeys.js";
import { registerEndpointPublish } from "./EndpointPublish.js";
import { requestDeleteEmailForUser, confirmDeleteAccount } from "./accountDelete.js";

import passport from "passport";


// ✅ S3 helpers (single import, consistent exports)
import {
  uploadDirToS3,
  uploadFileToS3,
  deletePrefixFromS3,
  deleteFromS3,
} from "./aws/s3Helpers.js";

const app = express();

/**
 * IMPORTANT for Render / proxies:
 * - allows secure cookies to work correctly behind Render’s proxy
 */
app.set("trust proxy", 1);

console.log("Using DB:", process.env.DATABASE_URL);

// -------------------------
// CORS
// -------------------------
/**
 * Put your allowed frontend origins in CLIENT_ORIGINS as comma-separated:
 * e.g.
 * CLIENT_ORIGINS=http://localhost:5173,https://mytube-frontend-xxx.vercel.app
 *
 * Tip: Vercel preview URLs change. Add them as needed, or use a stable Production domain.
 */
const allowedOrigins = new Set(
  (process.env.CLIENT_ORIGINS || "http://localhost:5173")
    .split(",")
    .map((s) => s.trim().replace(/\/$/, "")) // ✅ remove trailing slash
    .filter(Boolean)
);


app.post("/api/account/delete/request", requireAuth, async (req, res) => {
  try {
    await requestDeleteEmailForUser(req.user.id);
    res.json({
      ok: true,
      message: "A confirmation email has been sent. The link will expire shortly.",
    });
  } catch (err) {
    console.error("delete request failed:", err);
    res.status(500).json({ error: "Failed to send delete confirmation email." });
  }
});

app.post("/api/account/delete/confirm", async (req, res) => {
  try {
    const { token, password } = req.body || {};

    if (!token || !password) {
      return res.status(400).json({ error: "Missing token or password." });
    }

    await confirmDeleteAccount({ token, password });

    // optionally clear session cookie if one exists
    res.clearCookie("session_id", {
      httpOnly: true,
      sameSite: "lax",
      secure: true,
      path: "/",
    });

    return res.json({ ok: true });
  } catch (err) {
    console.error("delete confirm failed:", err);
    return res.status(400).json({ error: err.message || "Failed to delete account." });
  }
});


function makeAssetsS3Client() {
  const region =
    process.env.S3_ASSETS_REGION ||
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION;

  if (!region) {
    throw new Error("Missing env S3_ASSETS_REGION (or AWS_REGION) for assets bucket uploads");
  }

  const endpoint = process.env.S3_ASSETS_ENDPOINT || undefined;

  return new S3Client({
    region,
    ...(endpoint ? { endpoint } : {}),
  });
}

async function retry(fn, { attempts = 2, delayMs = 500 } = {}) {
  let lastErr;

  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;

      const retryable =
        err?.message?.includes("socket hang up") ||
        err?.code === "ECONNRESET" ||
        err?.code === "ETIMEDOUT";

      if (!retryable || i === attempts - 1) {
        throw err;
      }

      console.warn(`[retry] attempt ${i + 1} failed, retrying...`, err.message);

      await new Promise((r) => setTimeout(r, delayMs));
    }
  }

  throw lastErr;
}

async function uploadFileToAssetsBucket({ assetsS3, bucket, key, filePath, contentType }) {
  const Body = fs.createReadStream(filePath);

  await assetsS3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body,
      ContentType: contentType,
      CacheControl: "public, max-age=31536000, immutable",
    })
  );
}

function scoreVideo(v, context) {
  let score = 0;

  // 🔥 Recency (newer = better)
  const ageHours =
    (Date.now() - new Date(v.createdAt).getTime()) / (1000 * 60 * 60);
  score += Math.max(0, 48 - ageHours); // decay after 48h

  // 👀 Views (popularity)
  score += Math.log10((v.views || 0) + 1) * 5;

  // ⭐ Rating
  if (v.ratingAvg) {
    score += v.ratingAvg * 3;
  }

  // 🎯 Tag match (personalization)
  if (context?.preferredTags && v.tags) {
    const overlap = v.tags.filter((t) =>
      context.preferredTags.has(t)
    ).length;

    score += overlap * 10;
  }

  return score;
}

async function getUserPreferences(userId) {
  const result = await pool.query(`
    SELECT unnest(tags) AS tag
    FROM videos v
    JOIN video_ratings vr ON vr.video_id = v.id
    WHERE vr.user_id = $1 AND vr.rating >= 4
  `, [userId]);

  const tagCounts = new Map();

  for (const row of result.rows) {
    const tag = row.tag;
    tagCounts.set(tag, (tagCounts.get(tag) || 0) + 1);
  }

  return {
    preferredTags: new Set(
      [...tagCounts.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10)
        .map(([tag]) => tag)
    ),
  };
}

function expandOrigin(s) {
  const v = String(s || "").trim().replace(/\/$/, "");
  if (!v) return [];

  // If already has protocol, use as-is
  if (/^https?:\/\//i.test(v)) return [v];

  // Otherwise, accept it as a hostname and expand
  return [`https://${v}`, `http://${v}`];
}

const explicitOrigins = new Set(
  (process.env.CLIENT_ORIGINS || "")
    .split(",")
    .flatMap(expandOrigin)
    .map((s) => s.trim().replace(/\/$/, ""))
    .filter(Boolean)
);

console.log("CLIENT_ORIGINS raw:", process.env.CLIENT_ORIGINS);
console.log("CORS explicitOrigins:", [...explicitOrigins]);

function isAllowedVercelOrigin(origin) {
  try {
    const { hostname, protocol } = new URL(origin);

    // only allow https for Vercel
    if (protocol !== "https:") return false;

    // allow any vercel.app preview/prod for this project
    // examples:
    // mytube-frontend-omega.vercel.app
    // mytube-frontend-git-main-xxx.vercel.app
    if (hostname === "mytube-frontend.vercel.app") return true;
    if (hostname.startsWith("mytube-frontend-") && hostname.endsWith(".vercel.app")) return true;

    return false;
  } catch {
    return false;
  }
}

const corsOptions = {
  origin: (origin, cb) => {

    console.log("[CORS] origin:", JSON.stringify(origin));
    console.log("[CORS] explicit size:", explicitOrigins.size);
    console.log("[CORS] explicit has:", origin ? explicitOrigins.has(origin.replace(/\/$/, "")) : null);

    if (!origin) return cb(null, true);

    const o = origin.replace(/\/$/, "").replace(/\r/g, "");
    const ok = explicitOrigins.has(o) || isAllowedVercelOrigin(o);
    console.log("[CORS] origin:", origin, "=>", ok ? "ALLOW" : "BLOCK");

    // 1) allow explicit list (custom domains, etc)
    if (explicitOrigins.has(o)) return cb(null, true);

    // 2) allow vercel preview/prod matching pattern
    if (isAllowedVercelOrigin(o)) return cb(null, true);

    return cb(new Error(`CORS blocked origin: ${origin}`));
  },
  credentials: true,
  methods: ["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With"],
  exposedHeaders: ["Content-Length", "Content-Range"],
};

app.use(cors(corsOptions));

// ✅ make preflight match EVERYTHING reliably
app.options(/.*/, cors(corsOptions));



// -------------------------
// Middleware
// -------------------------
app.use(morgan("dev"));
app.use(express.json());
app.use(cookieParser());
app.use(passport.initialize());

// -------------------------
// Paths / storage
// -------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_ROOT = process.env.DATA_ROOT || path.join(os.tmpdir(), "mytube");
const THUMB_DIR = path.join(DATA_ROOT, "thumbs");
const VIDEO_DIR = path.join(DATA_ROOT, "videos");


fs.mkdirSync(THUMB_DIR, { recursive: true });
fs.mkdirSync(VIDEO_DIR, { recursive: true });

const VIDEO_SOURCE = process.env.VIDEO_SOURCE || "local"; // "local" | "aws"

// CloudFront base URLs (optional)
const CDN_UPLOADS_BASE_URL = (process.env.CDN_UPLOADS_BASE_URL || "").replace(/\/$/, "");
const CDN_ASSETS_BASE_URL = (process.env.CDN_ASSETS_BASE_URL || "").replace(/\/$/, "");

const APP_CURRENT_VERSION = process.env.APP_CURRENT_VERSION || "1.0.4-beta";

const LATEST_VERSION_MANIFEST = {
  latest_version: "1.0.5-beta",
  critical_update: false,
  engine_patch_url:
    "https://drive.google.com/uc?export=download&id=YOUR_GOOGLE_DRIVE_FILE_ID",
  release_notes:
    "Fixed FlashVSR variance explosion and improved warp blending.",
};

// -------------------------
// Session -> req.user
// -------------------------
async function getUserFromSession(req) {
  const sid = req.cookies?.session_id;
  if (!sid) return null;

  const result = await pool.query(
  `
  SELECT
    u.id,
    u.username,
    u.tokens,
    u.email_verified,
    u.is_moderator,
    u.is_banned,
    u.tier,
    COALESCE(AVG(vr.rating)::float, 0) AS rating,
    COALESCE(COUNT(vr.rating)::int, 0) AS review_count
  FROM sessions s
  JOIN users u ON u.id = s.user_id
  LEFT JOIN videos v ON v.user_id = u.id
  LEFT JOIN video_ratings vr ON vr.video_id = v.id
  WHERE s.id = $1 AND s.expires_at > now()
  GROUP BY u.id
  `,
  [sid]
);


  if (!result.rows[0]) return null;

  const u = result.rows[0];
  return {
    id: u.id,
    username: u.username,
    tokens: u.tokens,
    isBanned: !!u.is_banned,
    rating: u.rating,
    isModerator: !!u.is_moderator,
    reviewCount: u.review_count,
    tier: u.tier,
    emailVerified: !!u.email_verified,
  };
}

// attach req.user early
app.use(async (req, _res, next) => {
  try {
    req.user = await getUserFromSession(req);
  } catch {
    req.user = null;
  }
  next();
});

async function requireAuth(req, res, next) {
  const user = req.user ?? (await getUserFromSession(req));
  if (!user) return res.status(401).json({ error: "Not logged in" });
  req.user = user;
  next();
}

app.patch("/api/me/tier", requireAuth, requireNotBanned, async (req, res) => {
  const userId = Number(req.user.id);
  const tier = String(req.body?.tier || "").trim();

  const allowedTiers = new Set(["Free", "Watcher", "Basic", "Premium"]);

  if (!allowedTiers.has(tier)) {
    return res.status(400).json({ error: "Invalid tier" });
  }

  try {
    await pool.query(
      `
      UPDATE users
      SET
        tier = $2,
        plan_active = TRUE,
        plan_expiry =
          CASE
            WHEN plan_expiry IS NOT NULL AND plan_expiry > now()
              THEN date_trunc('hour', plan_expiry) + interval '1 month'
            ELSE date_trunc('hour', now()) + interval '1 month'
          END
      WHERE id = $1
      `,
      [userId, tier]
    );

    return res.json({ ok: true, tier });
  } catch (e) {
    console.error("PATCH /api/me/tier error:", e);
    return res.status(500).json({ error: "Failed to update tier" });
  }
});

async function requireNotBanned(req, res, next) {
  const user = req.user ?? (await getUserFromSession(req));
  if (!user) return res.status(401).json({ error: "Not logged in" });
  if (user.isBanned) {
    return res.status(403).json({ error: "This account has been banned." });
  }
  req.user = user;
  next();
}

app.get("/system/latest-version.json", async (req, res) => {
  const channel = String(req.query.channel || "beta").trim().toLowerCase();

  try {
    const result = await pool.query(
      `
      SELECT
        latest_version,
        patch_url,
        critical_update,
        release_notes
      FROM app_versions
      WHERE channel = $1
      LIMIT 1
      `,
      [channel]
    );

    const row = result.rows[0];

    if (!row) {
      return res.json({
        latest_version: "1.0.4-beta",
        critical_update: false,
        engine_patch_url: null,
        release_notes: "",
      });
    }

    return res.json({
      latest_version: row.latest_version,
      critical_update: !!row.critical_update,
      engine_patch_url: row.patch_url || null,
      release_notes: row.release_notes || "",
    });
  } catch (e) {
    console.error("GET /system/latest-version.json error:", e);
    return res.status(500).json({
      error: "Failed to load version manifest",
    });
  }
});

app.patch("/api/system/version", requireAuth, requireModerator, async (req, res) => {
  const channel = String(req.body?.channel || "beta").trim().toLowerCase();
  const currentVersion = String(req.body?.current_version || "").trim();
  const latestVersion = String(req.body?.latest_version || "").trim();
  const patchUrlRaw = String(req.body?.patch_url || "").trim();
  const releaseNotes = String(req.body?.release_notes || "");
  const criticalUpdate = !!req.body?.critical_update;

  if (!channel) {
    return res.status(400).json({ error: "channel is required" });
  }

  if (!currentVersion) {
    return res.status(400).json({ error: "current_version is required" });
  }

  if (!latestVersion) {
    return res.status(400).json({ error: "latest_version is required" });
  }

  const patchUrl = patchUrlRaw || null;

  try {
    const result = await pool.query(
      `
      INSERT INTO app_versions (
        channel,
        current_version,
        latest_version,
        patch_url,
        critical_update,
        release_notes,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, NOW())
      ON CONFLICT (channel)
      DO UPDATE SET
        current_version = EXCLUDED.current_version,
        latest_version = EXCLUDED.latest_version,
        patch_url = EXCLUDED.patch_url,
        critical_update = EXCLUDED.critical_update,
        release_notes = EXCLUDED.release_notes,
        updated_at = NOW()
      RETURNING
        channel,
        current_version,
        latest_version,
        patch_url,
        critical_update,
        release_notes,
        updated_at
      `,
      [
        channel,
        currentVersion,
        latestVersion,
        patchUrl,
        criticalUpdate,
        releaseNotes,
      ]
    );

    const row = result.rows[0];

    return res.json({
      ok: true,
      version: {
        channel: row.channel,
        current: row.current_version,
        latest: row.latest_version,
        patch_url: row.patch_url,
        critical_update: !!row.critical_update,
        release_notes: row.release_notes || "",
        updated_at: row.updated_at,
      },
    });
  } catch (e) {
    console.error("PATCH /api/system/version error:", e);
    return res.status(500).json({ error: "Failed to update version info" });
  }
});

app.get("/api/system/version", async (req, res) => {
  const channel = String(req.query.channel || "beta").trim().toLowerCase();

  try {
    const result = await pool.query(
      `
      SELECT
        current_version,
        latest_version,
        patch_url,
        critical_update,
        release_notes
      FROM app_versions
      WHERE channel = $1
      LIMIT 1
      `,
      [channel]
    );

    const row = result.rows[0];

    if (!row) {
      return res.json({
        current: "1.0.4-beta",
        latest: "1.0.4-beta",
        patch_url: null,
      });
    }

    return res.json({
      current: row.current_version,
      latest: row.latest_version,
      patch_url: row.patch_url || null,
      critical_update: !!row.critical_update,
      release_notes: row.release_notes || "",
    });
  } catch (e) {
    console.error("GET /api/system/version error:", e);
    return res.status(500).json({
      error: "Failed to load version info",
    });
  }
});

app.get("/api/me/media-key", requireAuth, async (req, res) => {
  try {
    const mediaKey = await getOrCreateUserMediaKey(req.user.id);
    return res.json({ mediaKey });
  } catch (e) {
    console.error("GET /api/me/media-key error:", e);
    return res.status(500).json({ error: "Failed to get media key" });
  }
});


app.get("/api/mod/reports", requireAuth, requireModerator, async (req, res) => {
  const archived = String(req.query.archived || "0") === "1";
  const q = String(req.query.q || "").trim();
  const limit = Math.min(Math.max(Number(req.query.limit || 100), 1), 100);
  const offset = Math.max(Number(req.query.offset || 0), 0);

  try {
    const where = [`r.status = $1`];
    const params = [archived ? "archived" : "open"];
    let i = 2;

    if (q) {
      where.push(`
        (
          CAST(v.id AS text) ILIKE $${i}
          OR v.title ILIKE $${i}
          OR CAST(v.user_id AS text) ILIKE $${i}
          OR u.username ILIKE $${i}
          OR COALESCE(p.display_name, '') ILIKE $${i}
          OR CAST(r.reporter_id AS text) ILIKE $${i}
          OR r.offense ILIKE $${i}
          OR COALESCE(r.comments, '') ILIKE $${i}
        )
      `);
      params.push(`%${q}%`);
      i++;
    }

    params.push(limit);
    params.push(offset);

    const result = await pool.query(
      `
      SELECT
        r.id,
        r.video_id,
        r.reporter_id,
        r.offense,
        r.comments,
        r.status,
        r.action_taken,
        r.addressed_at,
        r.created_at,

        v.title AS video_title,
        v.visibility,
        v.user_id AS video_owner_id,

        u.username AS channel_username,
        COALESCE(p.display_name, '') AS channel_display_name,

        ru.username AS reporter_username,
        COALESCE(rp.display_name, '') AS reporter_display_name
      FROM video_reports r
      JOIN videos v ON v.id::text = r.video_id::text
      JOIN users u ON u.id = v.user_id
      LEFT JOIN user_profiles p ON p.user_id = u.id
      LEFT JOIN users ru ON ru.id = r.reporter_id
      LEFT JOIN user_profiles rp ON rp.user_id = ru.id
      WHERE ${where.join(" AND ")}
      ORDER BY
        CASE WHEN r.status = 'open' THEN r.created_at END DESC,
        CASE WHEN r.status = 'archived' THEN r.addressed_at END DESC,
        r.id DESC
      LIMIT $${i} OFFSET $${i + 1}
      `,
      params
    );

    return res.json(
      result.rows.map((row) => ({
        id: row.id,
        videoId: row.video_id,
        videoTitle: row.video_title,
        visibility: row.visibility,

        userId: row.video_owner_id,
        username: row.channel_username,
        displayName: row.channel_display_name || row.channel_username,

        reporterId: row.reporter_id,
        reporterUsername: row.reporter_username || null,
        reporterDisplayName:
          row.reporter_display_name || row.reporter_username || null,

        offense: row.offense,
        comments: row.comments || "",
        status: row.status,
        actionTaken: row.action_taken || null,
        createdAt: row.created_at,
        addressedAt: row.addressed_at,
      }))
    );
  } catch (e) {
    console.error("GET /api/mod/reports error:", e);
    return res.status(500).json({ error: "Failed to load reports" });
  }
});

app.get("/api/mod/users", requireAuth, requireModerator, async (req, res) => {
  try {
    const q = String(req.query.q || "").trim();
    const filterBy = String(req.query.filterBy || "all");
    const limit = Math.min(50, Math.max(1, Number(req.query.limit) || 50));
    const offset = Math.max(0, Number(req.query.offset) || 0);

    const values = [limit, offset];
    let whereSql = "";

    if (q) {
      values.push(q);

      if (filterBy === "id") {
        whereSql = `WHERE CAST(u.id AS TEXT) ILIKE '%' || $3 || '%'`;
      } else if (filterBy === "username") {
        whereSql = `WHERE COALESCE(u.username, '') ILIKE '%' || $3 || '%'`;
      } else if (filterBy === "email") {
        whereSql = `WHERE COALESCE(u.email, '') ILIKE '%' || $3 || '%'`;
      } else if (filterBy === "displayName") {
        whereSql = `WHERE COALESCE(p.display_name, '') ILIKE '%' || $3 || '%'`;
      } else {
        whereSql = `
          WHERE
            CAST(u.id AS TEXT) ILIKE '%' || $3 || '%'
            OR COALESCE(u.username, '') ILIKE '%' || $3 || '%'
            OR COALESCE(u.email, '') ILIKE '%' || $3 || '%'
            OR COALESCE(p.display_name, '') ILIKE '%' || $3 || '%'
        `;
      }
    }

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.username,
        u.email,
        COALESCE(p.display_name, '') AS "displayName",
        u.tokens,
        u.is_moderator AS "isModerator",
        u.tier,
        u.is_comment_shadowbanned AS "isCommentShadowbanned",
        u.is_banned AS "isBanned",
        COALESCE(rs.rating_avg, 0) AS "ratingAvg",
        COALESCE(rs.rating_count, 0) AS "reviewCount"
      FROM users u
      LEFT JOIN user_profiles p
        ON p.user_id = u.id
      LEFT JOIN LATERAL (
        SELECT
          AVG(vr.rating)::numeric(10,2) AS rating_avg,
          COUNT(*)::int AS rating_count
        FROM videos v
        JOIN video_ratings vr
          ON vr.video_id::text = v.id::text
        WHERE v.user_id = u.id
      ) rs ON TRUE
      ${whereSql}
      ORDER BY u.id DESC
      LIMIT $1 OFFSET $2
      `,
      values
    );

    res.json(result.rows);
  } catch (err) {
    console.error("GET /api/mod/users failed", err);
    res.status(500).json({ error: "Failed to fetch users" });
  }
});

app.get("/api/mod/users/:id", requireAuth, requireModerator, async (req, res) => {
  try {
    const userId = req.params.id;

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.username,
        u.email,
        COALESCE(p.display_name, '') AS "displayName",
        u.tokens,
        u.is_moderator AS "isModerator",
        u.tier,
        u.is_comment_shadowbanned AS "isCommentShadowbanned",
        u.is_banned AS "isBanned",
        COALESCE(rs.rating_avg, 0) AS "ratingAvg",
        COALESCE(rs.rating_count, 0) AS "reviewCount"
      FROM users u
      LEFT JOIN user_profiles p
        ON p.user_id = u.id
      LEFT JOIN LATERAL (
        SELECT
          AVG(vr.rating)::numeric(10,2) AS rating_avg,
          COUNT(*)::int AS rating_count
        FROM videos v
        JOIN video_ratings vr
          ON vr.video_id::text = v.id::text
        WHERE v.user_id = u.id
      ) rs ON TRUE
      WHERE u.id = $1
      LIMIT 1
      `,
      [userId]
    );

    if (!result.rows[0]) {
      return res.status(404).json({ error: "User not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error("GET /api/mod/users/:id failed", err);
    res.status(500).json({ error: "Failed to fetch user" });
  }
});

app.patch("/api/mod/users/:id", requireAuth, requireModerator, async (req, res) => {
  try {
    const userId = req.params.id;

    let {
      tokens,
      tier,
      isModerator,
      isCommentShadowbanned,
      isBanned,
    } = req.body || {};

    if (!Number.isFinite(Number(tokens)) || Number(tokens) < 0) {
      return res.status(400).json({ error: "Invalid tokens" });
    }

    const allowedTiers = new Set(["Free", "Watcher", "Basic", "Premium"]);
    if (!allowedTiers.has(String(tier || "Free"))) {
      return res.status(400).json({ error: "Invalid plan" });
    }

    isModerator = Boolean(isModerator);
    isCommentShadowbanned = Boolean(isCommentShadowbanned);
    isBanned = Boolean(isBanned);

    if (isBanned) {
      tier = "Free";
    }

    const result = await pool.query(
      `
      UPDATE users
      SET
        tokens = $2,
        tier = $3,
        is_moderator = $4,
        is_comment_shadowbanned = $5,
        is_banned = $6,
        plan_active = CASE WHEN $6 THEN FALSE ELSE plan_active END,
        plan_expiry = CASE WHEN $6 THEN NULL ELSE plan_expiry END
      WHERE id = $1
      RETURNING
        id,
        username,
        email,
        tokens,
        is_moderator AS "isModerator",
        tier,
        is_comment_shadowbanned AS "isCommentShadowbanned",
        is_banned AS "isBanned"
      `,
      [
        userId,
        Math.floor(Number(tokens)),
        tier,
        isModerator,
        isCommentShadowbanned,
        isBanned,
      ]
    );

    if (!result.rows[0]) {
      return res.status(404).json({ error: "User not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error("PATCH /api/mod/users/:id failed", err);
    res.status(500).json({ error: "Failed to update moderation user" });
  }
});



app.post("/api/me/renew-plan", requireAuth, requireNotBanned, async (req, res) => {
  const userId = Number(req.user.id);

  try {
    const result = await pool.query(
      `
      UPDATE users
      SET plan_active = TRUE
      WHERE id = $1
        AND tier <> 'Free'
        AND plan_active = FALSE
        AND plan_expiry IS NOT NULL
        AND plan_expiry > now()
      RETURNING tier, plan_active, plan_expiry
      `,
      [userId]
    );

    if (!result.rows.length) {
      return res.status(400).json({
        error: "No cancelled active plan to renew",
      });
    }

    return res.json({
      ok: true,
      message: "Subscription renewed. Auto-renewal is active again.",
      tier: result.rows[0].tier,
      planActive: !!result.rows[0].plan_active,
      planExpiry: result.rows[0].plan_expiry,
    });
  } catch (e) {
    console.error("POST /api/me/renew-plan error:", e);
    return res.status(500).json({ error: "Failed to renew subscription" });
  }
});

app.post("/api/me/cancel-plan", requireAuth, async (req, res) => {
  const userId = Number(req.user.id);

  try {
    const result = await pool.query(
      `
      UPDATE users
      SET plan_active = FALSE
      WHERE id = $1
        AND tier <> 'Free'
      RETURNING tier, plan_active, plan_expiry
      `,
      [userId]
    );

    if (!result.rows.length) {
      return res.status(400).json({
        error: "No active paid plan to cancel",
      });
    }

    return res.json({
      ok: true,
      message:
        "Subscription cancelled. Your plan will remain active until the end of the billing period.",
      tier: result.rows[0].tier,
      planActive: !!result.rows[0].plan_active,
      planExpiry: result.rows[0].plan_expiry,
    });
  } catch (e) {
    console.error("POST /api/me/cancel-plan error:", e);
    return res.status(500).json({ error: "Failed to cancel subscription" });
  }
});

app.post("/api/mod/reports/:reportId/resolve", requireAuth, requireModerator, async (req, res) => {
  const reportId = Number(req.params.reportId);
  const action = String(req.body?.action || "").trim().toLowerCase();

  if (!Number.isFinite(reportId)) {
    return res.status(400).json({ error: "Invalid report id" });
  }

  if (!["pass", "fail"].includes(action)) {
    return res.status(400).json({ error: "Action must be pass or fail" });
  }

  try {
    await pool.query("BEGIN");

    const reportRes = await pool.query(
      `
      SELECT r.id, r.video_id, r.status
      FROM video_reports r
      WHERE r.id = $1
      LIMIT 1
      `,
      [reportId]
    );

    const report = reportRes.rows[0];
    if (!report) {
      await pool.query("ROLLBACK");
      return res.status(404).json({ error: "Report not found" });
    }

    if (report.status !== "open") {
      await pool.query("ROLLBACK");
      return res.status(400).json({ error: "Report already addressed" });
    }

    if (action === "fail") {
      await pool.query(
        `
        UPDATE videos
        SET visibility = 'blacklisted',
            updated_at = NOW()
        WHERE id::text = $1::text
        `,
        [String(report.video_id)]
      );
    }

    await pool.query(
      `
      UPDATE video_reports
      SET
        status = 'archived',
        action_taken = $2,
        addressed_at = NOW()
      WHERE id = $1
      `,
      [reportId, action]
    );

    await pool.query("COMMIT");
    return res.json({ ok: true, id: reportId, action });
  } catch (e) {
    await pool.query("ROLLBACK").catch(() => {});
    console.error("POST /api/mod/reports/:reportId/resolve error:", e);
    return res.status(500).json({ error: "Failed to resolve report" });
  }
});

async function requireModerator(req, res, next) {
  const user = req.user ?? (await getUserFromSession(req));
  if (!user) return res.status(401).json({ error: "Not logged in" });
  if (!user.isModerator) return res.status(403).json({ error: "Forbidden" });
  req.user = user;
  next();
}

// -------------------------
// Routers
// -------------------------
/**
 * Support BOTH paths so your frontend can hit either:
 * - /auth/login
 * - /api/auth/login
 *
 * Your console earlier showed /auth/me + /auth/login, so this avoids 404s.
 */
app.use("/auth", authRouter);
app.use("/api/auth", authRouter);

app.use("/api/profile", profileRouter);

// -------------------------
// Upload (multer) -> always to local disk first
// -------------------------
const upload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, VIDEO_DIR),
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname).toLowerCase() || ".mp4";
      const name = `${Date.now()}-${crypto.randomBytes(8).toString("hex")}${ext}`;
      cb(null, name);
    },
  }),
  limits: { fileSize: 1024 * 1024 * 1024 }, // 1GB

  fileFilter: (_req, file, cb) => {
    const mimetype = String(file.mimetype || "");
    const ext = path.extname(file.originalname || "").toLowerCase();

    const commonVideoExts = new Set([".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v", ".mpeg", ".mpg"]);
    const commonAudioExts = new Set([".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".opus"]);

    const ok =
      mimetype.startsWith("video/") ||
      mimetype.startsWith("audio/") ||
      commonVideoExts.has(ext) ||
      commonAudioExts.has(ext);

    cb(ok ? null : new Error("Only video/audio files allowed"), ok);
  },
});



// -------------------------
// Subscriptions
// -------------------------

app.post("/api/channels/:channelId/subscribe", requireVerifiedEmail, requireNotBanned, async (req, res) => {
  const subscriberId = Number(req.user.id);
  const channelId = Number(req.params.channelId);

  if (!Number.isInteger(channelId) || channelId <= 0) {
    return res.status(400).json({ error: "Invalid channel id" });
  }

  if (subscriberId === channelId) {
    return res.status(400).json({ error: "You cannot subscribe to yourself" });
  }

  try {
    const exists = await pool.query(
      `SELECT id, username FROM users WHERE id = $1 LIMIT 1`,
      [channelId]
    );

    if (!exists.rows[0]) {
      return res.status(404).json({ error: "Channel not found" });
    }

    await pool.query(
      `
      INSERT INTO subscriptions (subscriber_id, channel_id)
      VALUES ($1, $2)
      ON CONFLICT DO NOTHING
      `,
      [subscriberId, channelId]
    );

    const countResult = await pool.query(
      `
      SELECT COUNT(*)::int AS count
      FROM subscriptions
      WHERE channel_id = $1
      `,
      [channelId]
    );

    return res.json({
      ok: true,
      subscribed: true,
      subscriberCount: countResult.rows[0].count,
    });
  } catch (err) {
    console.error("Subscribe error:", err);
    return res.status(500).json({ error: "Failed to subscribe" });
  }
});

app.delete("/api/channels/:channelId/subscribe", requireAuth, async (req, res) => {
  const subscriberId = Number(req.user.id);
  const channelId = Number(req.params.channelId);

  if (!Number.isInteger(channelId) || channelId <= 0) {
    return res.status(400).json({ error: "Invalid channel id" });
  }

  try {
    await pool.query(
      `
      DELETE FROM subscriptions
      WHERE subscriber_id = $1 AND channel_id = $2
      `,
      [subscriberId, channelId]
    );

    const countResult = await pool.query(
      `
      SELECT COUNT(*)::int AS count
      FROM subscriptions
      WHERE channel_id = $1
      `,
      [channelId]
    );

    return res.json({
      ok: true,
      subscribed: false,
      subscriberCount: countResult.rows[0].count,
    });
  } catch (err) {
    console.error("Unsubscribe error:", err);
    return res.status(500).json({ error: "Failed to unsubscribe" });
  }
});

app.get("/api/channels/:channelId/subscription", async (req, res) => {
  const channelId = Number(req.params.channelId);

  if (!Number.isInteger(channelId) || channelId <= 0) {
    return res.status(400).json({ error: "Invalid channel id" });
  }

  try {
    const countResult = await pool.query(
      `
      SELECT COUNT(*)::int AS count
      FROM subscriptions
      WHERE channel_id = $1
      `,
      [channelId]
    );

    let subscribed = false;

    if (req.user?.id) {
      const subResult = await pool.query(
        `
        SELECT 1
        FROM subscriptions
        WHERE subscriber_id = $1 AND channel_id = $2
        LIMIT 1
        `,
        [req.user.id, channelId]
      );

      subscribed = subResult.rowCount > 0;
    }

    return res.json({
      subscribed,
      subscriberCount: countResult.rows[0].count,
    });
  } catch (err) {
    console.error("Subscription status error:", err);
    return res.status(500).json({ error: "Failed to fetch subscription status" });
  }
});

app.get("/api/me/subscriptions", requireAuth, async (req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT
        u.id,
        u.username,
        u.username AS display_name,
        NULL AS avatar_url,
        s.created_at
      FROM subscriptions s
      JOIN users u ON u.id = s.channel_id
      WHERE s.subscriber_id = $1
      ORDER BY s.created_at DESC
      `,
      [req.user.id]
    );

    return res.json(result.rows);
  } catch (err) {
    console.error("My subscriptions error:", err);
    return res.status(500).json({ error: "Failed to fetch subscriptions" });
  }
});


// -------------------------
// FFMPEG helpers
// -------------------------
function runCmd(cmd, args) {
  // Print the exact command + arguments
  console.log("RUN:", cmd, args.map((a) => (a === undefined ? "undefined" : JSON.stringify(a))).join(" "));

  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { windowsHide: true });

    let out = "";
    let err = "";

    p.stdout.on("data", (d) => (out += d.toString()));
    p.stderr.on("data", (d) => (err += d.toString()));

    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) resolve({ out, err });
      else reject(new Error(err || `${cmd} exited with code ${code}`));
    });
  });
}

  async function requireVerifiedEmail(req, res, next) {
    const user = req.user ?? (await getUserFromSession(req));
    if (!user) return res.status(401).json({ error: "Not logged in" });
    if (user.isBanned) {
      return res.status(403).json({ error: "This account has been banned." });
    }
    if (!user.emailVerified) {
      return res.status(403).json({ error: "Please verify your email first" });
    }
    req.user = user;
    next();
  }

async function generateHls(inputPath, outDir) {
  fs.mkdirSync(outDir, { recursive: true });

  // Important: normalize to forward slashes for ffmpeg/HLS manifests
  const outDirPosix = outDir.replace(/\\/g, "/");

  const segPattern = `${outDirPosix}/v%v/seg_%05d.ts`;
  const playlistPattern = `${outDirPosix}/v%v/playlist.m3u8`;

  const vf0 =
    "scale=854:480:force_original_aspect_ratio=decrease," +
    "pad=854:480:(854-iw)/2:(480-ih)/2," +
    "setsar=1,format=yuv420p";

  const vf1 =
    "scale=1280:720:force_original_aspect_ratio=decrease," +
    "pad=1280:720:(1280-iw)/2:(720-ih)/2," +
    "setsar=1,format=yuv420p";

  const args = [
    "-y",
    "-i", inputPath,

    // Variant 0
    "-map", "0:v:0",
    "-map", "0:a?",
    "-filter:v:0", vf0,
    "-c:v:0", "libx264",
    "-profile:v:0", "main",
    "-crf:v:0", "20",
    "-preset", "veryfast",
    "-g", "48",
    "-keyint_min", "48",
    "-sc_threshold", "0",

    // Variant 1
    "-map", "0:v:0",
    "-map", "0:a?",
    "-filter:v:1", vf1,
    "-c:v:1", "libx264",
    "-profile:v:1", "main",
    "-crf:v:1", "20",
    "-preset", "veryfast",
    "-g", "48",
    "-keyint_min", "48",
    "-sc_threshold", "0",

    // Audio duplicate
    "-c:a:0", "aac",
    "-b:a:0", "128k",
    "-ac:a:0", "2",
    "-c:a:1", "aac",
    "-b:a:1", "128k",
    "-ac:a:1", "2",

    // HLS
    "-f", "hls",
    "-hls_time", "4",
    "-hls_playlist_type", "vod",
    "-hls_flags", "independent_segments",
    "-hls_segment_type", "mpegts",
    "-hls_segment_filename", segPattern,
    "-master_pl_name", "master.m3u8",
    "-var_stream_map", "v:0,a:0 v:1,a:1",
    playlistPattern,
  ];

  await runCmd("ffmpeg", args);
}

async function getVideoDurationSeconds(videoPath) {
  try {
    const { out } = await runCmd("ffprobe", [
      "-v",
      "error",
      "-show_entries",
      "format=duration",
      "-of",
      "default=noprint_wrappers=1:nokey=1",
      videoPath,
    ]);
    const dur = Number(String(out).trim());
    if (!Number.isFinite(dur) || dur <= 0) return null;
    return dur;
  } catch {
    return null;
  }
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

async function generateThumbnailAtSecond(videoPath, thumbPath, seconds) {
  await runCmd("ffmpeg", [
    "-y",
    "-hide_banner",
    "-loglevel",
    "error",
    "-ss",
    String(seconds),
    "-i",
    videoPath,
    "-frames:v",
    "1",
    "-vf",
    "scale=640:-1",
    "-q:v",
    "3",
    thumbPath,
  ]);
}

async function generateThumbnailHalfwayWithFallback(videoPath, thumbPath) {
  const dur = await getVideoDurationSeconds(videoPath);

  let candidates = [];
  if (dur && dur > 0) {
    const half = clamp(Math.floor(dur * 0.5), 1, Math.max(1, Math.floor(dur - 1)));
    candidates = [half, 30, 10, 3, 1]
      .map((t) => clamp(Number(t), 0, Math.max(0, Math.floor(dur - 0.25))))
      .filter((t) => Number.isFinite(t) && t >= 0);
    candidates = Array.from(new Set(candidates));
  } else {
    candidates = [30, 10, 3, 1];
  }

  let lastErr = null;
  for (const t of candidates) {
    try {
      await generateThumbnailAtSecond(videoPath, thumbPath, t);
      return { ok: true, usedSecond: t, duration: dur };
    } catch (e) {
      lastErr = e;
      try {
        if (fs.existsSync(thumbPath)) fs.unlinkSync(thumbPath);
      } catch {}
    }
  }
  throw lastErr || new Error("Thumbnail generation failed");
}

// -------------------------
// HLS generation (single VOD rendition)
// -------------------------
async function generateHlsVOD(inputPath, outDir) {
  fs.mkdirSync(outDir, { recursive: true });

  const args = [
    "-y",
    "-hide_banner",
    "-loglevel",
    "error",
    "-i",
    inputPath,
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-crf",
    "22",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    "-ac",
    "2",
    "-f",
    "hls",
    "-hls_time",
    "6",
    "-hls_playlist_type",
    "vod",
    "-hls_flags",
    "independent_segments",
    "-hls_segment_type",
    "mpegts",
    "-hls_segment_filename",
    path.join(outDir, "seg_%03d.ts"),
    path.join(outDir, "master.m3u8"),
  ];

  await runCmd("ffmpeg", args);
}

// -------------------------
// Rating stats helper (resilient)
// -------------------------
async function getRatingStats(videoId) {
  try {
    const result = await pool.query(
      `
      SELECT rating_avg, rating_count
      FROM video_rating_stats
      WHERE video_id::text = $1::text
      `,
      [String(videoId)]
    );

    if (result.rows.length === 0) return { ratingAvg: null, ratingCount: 0 };

    return {
      ratingAvg: Number(result.rows[0].rating_avg),
      ratingCount: Number(result.rows[0].rating_count),
    };
  } catch (e) {
    console.warn("getRatingStats failed (fallback):", e.message);
    return { ratingAvg: null, ratingCount: 0 };
  }
}

async function fetchVideoById(videoId) {
  const result = await pool.query(
    `
    SELECT
      v.id,
      v.user_id,
      v.title,
      v.description,
      v.category,
      v.visibility,
      v.media_type,
      v.asset_scope,
      v.filename,
      v.filename,
      v.thumb,
      v.duration_text,
      v.duration,
      v.views,
      v.tags,
      v.asset_scope,
      v.media_type,
      v.created_at AS "createdAt",
      v.updated_at AS "updatedAt",
      u.username AS channel_username,
      COALESCE(p.display_name, '') AS channel_display_name
    FROM videos v
    JOIN users u ON u.id = v.user_id
    LEFT JOIN user_profiles p ON p.user_id = u.id
    WHERE v.id::text = $1::text
    LIMIT 1
    `,
    [String(videoId)]
  );

  return result.rows[0] || null;
}

function baseUrl(req) {
  return `${req.protocol}://${req.get("host")}`;
}

async function toApiVideo(req, v) {
  const b = baseUrl(req);
  const { ratingAvg, ratingCount } = await getRatingStats(v.id);

  let playbackUrl = `${b}/videos/${v.id}/stream`;

  if (VIDEO_SOURCE === "aws") {
    if (CDN_UPLOADS_BASE_URL) {
      playbackUrl = `${CDN_UPLOADS_BASE_URL}/${v.filename}`;
    } else if (process.env.S3_UPLOADS_BUCKET && process.env.AWS_REGION) {
      playbackUrl = `https://${process.env.S3_UPLOADS_BUCKET}.s3.${process.env.AWS_REGION}.amazonaws.com/${v.filename}`;
    }
  }

  const thumbUrl =
    VIDEO_SOURCE === "aws" &&
    v.thumb &&
    v.thumb !== "placeholder.jpg" &&
    CDN_ASSETS_BASE_URL
      ? `${CDN_ASSETS_BASE_URL}/${v.thumb}`
      : v.thumb
      ? `${b}/thumbs/${v.thumb}`
      : `${b}/thumbs/placeholder.jpg`;

  return {
    id: v.id,
    title: v.title,
    description: v.description || "",
    category: v.category || "Other",
    visibility: v.visibility || "public",

    channelUsername: v.channel_username,
    channelDisplayName: v.channel_display_name || v.channel_username,

    createdAt: v.createdAt,
    updatedAt: v.updatedAt,
    views: v.views ?? null,

    durationText: v.duration_text || null,
    durationSeconds: Number(v.duration || 0),

    progressSeconds: Number(v.progress_seconds || 0),

    tags: Array.isArray(v.tags) ? v.tags : [],
    mediaType: v.media_type || "video",
    assetScope: v.asset_scope || "public",

    ratingAvg,
    ratingCount,

    thumbUrl,
    playbackUrl,
  };
}

function uniqueById(rows = []) {
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const id = String(row?.id ?? "");
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(row);
  }
  return out;
}

async function enrichRows(req, rows) {
  return Promise.all(rows.map((v) => toApiVideo(req, v)));
}

function filterOutSeenVideos(rows = [], seenIds = new Set()) {
  const out = [];

  for (const row of rows) {
    const id = String(row?.id ?? "");
    if (!id || seenIds.has(id)) continue;
    seenIds.add(id);
    out.push(row);
  }

  return out;
}

async function buildHomeRow({
  req,
  key,
  title,
  dbRows,
  seenIds,
  minVideos = 1,
  allowDuplicates = false,
  limit = 12,
  context = null, // 👈 NEW
}) {
  const uniqueRows = uniqueById(dbRows);

  let filteredRows = allowDuplicates
    ? uniqueRows
    : uniqueRows.filter((row) => !seenIds.has(String(row.id)));

  // 🔥 SCORE + SORT
  if (context) {
    filteredRows = filteredRows
      .map((v) => ({ ...v, __score: scoreVideo(v, context) }))
      .sort((a, b) => b.__score - a.__score);
  }

  filteredRows = filteredRows.slice(0, limit);

  if (filteredRows.length < minVideos) return null;

  const videos = await enrichRows(req, filteredRows);
  if (!videos.length) return null;

  if (!allowDuplicates) {
    for (const row of filteredRows) {
      seenIds.add(String(row.id));
    }
  }

  return { key, title, videos };
}

registerEndpointPublish(app, {
  pool,
  requireAuth,
  uploadFileToS3,
});

registerGenerateProjects(app, {
  pool,
  requireAuth,
  requireVerifiedEmail,
  requireNotBanned,
});

registerGeneratePublish(app, {
  pool,
  requireAuth,
  requireVerifiedEmail,
  requireNotBanned,
  uploadFileToS3,
  VIDEO_SOURCE,
  VIDEO_DIR,
});

app.get("/__ffmpeg", async (_req, res) => {
  try {
    const r = await runCmd("ffmpeg", ["-version"]);
    res.json({ ok: true, ffmpeg: (r.out || r.err).slice(0, 500) });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// -------------------------
// USER VIDEOS (Profile page needs this)
// GET /api/profile/u/:username/videos?sort=newest|oldest|views|highest
app.get("/api/profile/u/:username/videos", async (req, res) => {
  try {
    const scope = String(req.query.scope || "").toLowerCase().trim(); // "public"|"library"|"" (default)
    const type = String(req.query.type || "").toLowerCase().trim();   // "video"|"audio"|"all"|"" (default)
    const username = String(req.params.username || "").trim();
    const sort = String(req.query.sort || "newest").toLowerCase().trim();

    if (!username) return res.status(400).json({ error: "Missing username" });

    // Find the user (case-insensitive)
    const u = await pool.query(
      `SELECT id, username FROM users WHERE lower(username) = lower($1) LIMIT 1`,
      [username]
    );
    if (!u.rows.length) return res.status(404).json({ error: "User not found" });

    const channelUserId = Number(u.rows[0].id);

    // If viewing your own profile, include private/unlisted too
    const requesterId = req.user?.id ? Number(req.user.id) : null;
    const includeAll = requesterId != null && requesterId === channelUserId;
    if (scope === "library" && !includeAll) {
      return res.status(403).json({ error: "Library assets are owner-only" });
    }

    // Sorting
    let orderBy = "v.created_at DESC";
    if (sort === "oldest") {
      orderBy = "v.created_at ASC";
    } else if (sort === "views") {
      orderBy = "v.views DESC NULLS LAST, v.created_at DESC";
    } else if (sort === "highest") {
      orderBy =
        "COALESCE(vrs.rating_avg, 0) DESC, COALESCE(vrs.rating_count, 0) DESC, v.created_at DESC";
    }

    // Fetch videos for that user
    const result = await pool.query(
      `
      SELECT
        v.id,
        v.user_id,
        v.title,
        v.description,
        v.category,
        v.visibility,
        v.media_type,
        v.asset_scope,
        v.filename,
        v.thumb,
        v.duration_text,
        v.views,
        v.tags,
        v.created_at AS "createdAt",
        v.updated_at AS "updatedAt",
        u.username AS channel_username,
        COALESCE(p.display_name, '') AS channel_display_name
      FROM videos v
      JOIN users u ON u.id = v.user_id
      LEFT JOIN user_profiles p ON p.user_id = u.id
      LEFT JOIN video_rating_stats vrs ON vrs.video_id = v.id
      WHERE v.user_id = $1
        AND ($2::boolean = true OR v.visibility = 'public')
        AND ($3::text = '' OR v.asset_scope = $3::text)
        AND ($4::text = '' OR $4::text = 'all' OR v.media_type = $4::text)
      ORDER BY ${orderBy}
      LIMIT 200
      `,
      [
        channelUserId,
        includeAll,
        scope || "",        // "" means no filter
        type || "",         // "" means no filter
      ]
    );

    // Return the SAME shape used everywhere else (thumbUrl + playbackUrl, etc.)
    const enriched = await Promise.all(result.rows.map((v) => toApiVideo(req, v)));
    return res.json(enriched); // IMPORTANT: return an array
  } catch (e) {
    console.error("GET /api/profile/u/:username/videos error:", e);
    return res.status(500).json({ error: "Failed to load user videos" });
  }
});


// -------------------------
// Watch Later
// -------------------------

app.get("/api/me/watch-later", requireAuth, async (req, res) => {
  const userId = Number(req.user.id);

  try {
    const result = await pool.query(
      `
      SELECT
        v.id,
        v.user_id,
        v.title,
        v.description,
        v.category,
        v.visibility,
        v.media_type,
        v.asset_scope,
        v.filename,
        v.thumb,
        v.duration_text,
        v.duration,
        v.views,
        v.tags,
        v.created_at AS "createdAt",
        v.updated_at AS "updatedAt",
        u.username AS channel_username,
        COALESCE(p.display_name, '') AS channel_display_name,
        wl.created_at AS saved_at
      FROM watch_later wl
      JOIN videos v ON v.id::text = wl.video_id::text
      JOIN users u ON u.id = v.user_id
      LEFT JOIN user_profiles p ON p.user_id = u.id
      WHERE wl.user_id = $1
        AND v.visibility = 'public'
        AND v.asset_scope = 'public'
        AND v.media_type = 'video'
      ORDER BY wl.created_at DESC
      LIMIT 200
      `,
      [userId]
    );

    const enriched = await Promise.all(
      result.rows.map(async (v) => {
        const apiVideo = await toApiVideo(req, v);
        return {
          ...apiVideo,
          savedAt: v.saved_at || null,
        };
      })
    );

    return res.json(enriched);
  } catch (e) {
    console.error("GET /api/me/watch-later error:", e);
    return res.status(500).json({ error: "Failed to load Watch Later" });
  }
});

app.get("/api/me/watch-later/:videoId", requireAuth, async (req, res) => {
  const userId = Number(req.user.id);
  const videoId = String(req.params.videoId);

  try {
    const result = await pool.query(
      `
      SELECT created_at
      FROM watch_later
      WHERE user_id = $1 AND video_id = $2
      LIMIT 1
      `,
      [userId, videoId]
    );

    return res.json({
      saved: result.rowCount > 0,
      savedAt: result.rows[0]?.created_at ?? null,
    });
  } catch (e) {
    console.error("GET /api/me/watch-later/:videoId error:", e);
    return res.status(500).json({ error: "Failed to load Watch Later status" });
  }
});

app.post("/api/me/watch-later/:videoId", requireVerifiedEmail, requireNotBanned, async (req, res) => {
  const userId = Number(req.user.id);
  const videoId = String(req.params.videoId);

  try {
    const exists = await pool.query(
      `SELECT id FROM videos WHERE id::text = $1::text LIMIT 1`,
      [videoId]
    );

    if (!exists.rows.length) {
      return res.status(404).json({ error: "Video not found" });
    }

    await pool.query(
      `
      INSERT INTO watch_later (user_id, video_id)
      VALUES ($1, $2)
      ON CONFLICT (user_id, video_id) DO NOTHING
      `,
      [userId, videoId]
    );

    return res.json({ ok: true, saved: true, videoId });
  } catch (e) {
    console.error("POST /api/me/watch-later/:videoId error:", e);
    return res.status(500).json({ error: "Failed to add to Watch Later" });
  }
});

app.delete("/api/me/watch-later/:videoId", requireAuth, async (req, res) => {
  const userId = Number(req.user.id);
  const videoId = String(req.params.videoId);

  try {
    await pool.query(
      `
      DELETE FROM watch_later
      WHERE user_id = $1 AND video_id = $2
      `,
      [userId, videoId]
    );

    return res.json({ ok: true, saved: false, videoId });
  } catch (e) {
    console.error("DELETE /api/me/watch-later/:videoId error:", e);
    return res.status(500).json({ error: "Failed to remove from Watch Later" });
  }
});

app.get("/api/home", async (req, res) => {
  try {
    const userId = req.user?.id ? String(req.user.id) : null;
    const rows = [];
    const seenIds = new Set();

    let context = null;

    if (userId) {
      context = await getUserPreferences(userId);
}

    console.log("[/api/home] start", { userId });

    // -------------------------
    // Continue Watching
    // -------------------------
    if (userId) {
      const continueRows = await pool.query(
        `
        SELECT
          v.id,
          v.user_id,
          v.title,
          v.description,
          v.category,
          v.visibility,
          v.media_type,
          v.asset_scope,
          v.filename,
          v.thumb,
          v.duration_text,
          v.duration,
          v.views,
          v.tags,
          v.created_at AS "createdAt",
          v.updated_at AS "updatedAt",
          u.username AS channel_username,
          COALESCE(p.display_name, '') AS channel_display_name,
          wh.watched_at,
          wh.progress_seconds
        FROM watch_history wh
        JOIN videos v ON v.id::text = wh.video_id::text
        JOIN users u ON u.id = v.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        WHERE wh.user_id::text = $1::text
          AND v.visibility = 'public'
          AND v.asset_scope = 'public'
          AND v.media_type = 'video'
        ORDER BY wh.watched_at DESC
        LIMIT 12
        `,
        [userId]
      );

      const row = await buildHomeRow({
        req,
        key: "continue-watching",
        title: "Continue Watching",
        dbRows: continueRows.rows,
        seenIds,
        minVideos: 1,
        context,
      });

      if (row) rows.push(row);
    }

    // -------------------------
    // From Your Subscriptions
    // -------------------------
    if (userId) {
      const subscriptionRows = await pool.query(
        `
        SELECT
          v.id,
          v.user_id,
          v.title,
          v.description,
          v.category,
          v.visibility,
          v.media_type,
          v.asset_scope,
          v.filename,
          v.thumb,
          v.duration_text,
          v.views,
          v.tags,
          v.created_at AS "createdAt",
          v.updated_at AS "updatedAt",
          u.username AS channel_username,
          COALESCE(p.display_name, '') AS channel_display_name
        FROM subscriptions s
        JOIN videos v ON v.user_id::text = s.channel_id::text
        JOIN users u ON u.id = v.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        WHERE s.subscriber_id::text = $1::text
          AND v.visibility = 'public'
          AND v.asset_scope = 'public'
          AND v.media_type = 'video'
        ORDER BY v.created_at DESC
        LIMIT 16
        `,
        [userId]
      );

      const row = await buildHomeRow({
        req,
        key: "from-subscriptions",
        title: "From Your Subscriptions",
        dbRows: subscriptionRows.rows,
        seenIds,
        minVideos: 4,
      });

      if (row) rows.push(row);
    }

    // -------------------------
    // Because You Rated Similar Videos
    // -------------------------
    if (userId) {
      const ratedTagRows = await pool.query(
        `
        WITH liked_videos AS (
          SELECT v.tags::text[] AS tags
          FROM video_ratings vr
          JOIN videos v ON v.id::text = vr.video_id::text
          WHERE vr.user_id::text = $1::text
            AND vr.rating >= 4
            AND v.tags IS NOT NULL
        ),
        top_tags AS (
          SELECT tag, COUNT(*)::int AS score
          FROM liked_videos,
               unnest(COALESCE(tags, ARRAY[]::text[])) AS tag
          GROUP BY tag
          ORDER BY score DESC, tag ASC
          LIMIT 5
        )
        SELECT
          v.id,
          v.user_id,
          v.title,
          v.description,
          v.category,
          v.visibility,
          v.media_type,
          v.asset_scope,
          v.filename,
          v.thumb,
          v.duration_text,
          v.views,
          v.tags,
          v.created_at AS "createdAt",
          v.updated_at AS "updatedAt",
          u.username AS channel_username,
          COALESCE(p.display_name, '') AS channel_display_name
        FROM videos v
        JOIN users u ON u.id = v.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        WHERE v.visibility = 'public'
          AND v.asset_scope = 'public'
          AND v.media_type = 'video'
          AND EXISTS (
            SELECT 1
            FROM top_tags tt
            WHERE tt.tag = ANY(COALESCE(v.tags::text[], ARRAY[]::text[]))
          )
          AND NOT EXISTS (
            SELECT 1
            FROM watch_history wh
            WHERE wh.user_id::text = $1::text
              AND wh.video_id::text = v.id::text
          )
        ORDER BY v.views DESC NULLS LAST, v.created_at DESC
        LIMIT 16
        `,
        [userId]
      );

      const row = await buildHomeRow({
        req,
        key: "based-on-ratings",
        title: "Because You Rated Similar Videos",
        dbRows: ratedTagRows.rows,
        seenIds,
        minVideos: 4,
      });

      if (row) rows.push(row);
    }

    // -------------------------
    // Trending
    // -------------------------
    {
      const trendingRows = await pool.query(
        `
        SELECT
          v.id,
          v.user_id,
          v.title,
          v.description,
          v.category,
          v.visibility,
          v.media_type,
          v.asset_scope,
          v.filename,
          v.thumb,
          v.duration_text,
          v.views,
          v.tags,
          v.created_at AS "createdAt",
          v.updated_at AS "updatedAt",
          u.username AS channel_username,
          COALESCE(p.display_name, '') AS channel_display_name
        FROM videos v
        JOIN users u ON u.id = v.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        WHERE v.visibility = 'public'
          AND v.asset_scope = 'public'
          AND v.media_type = 'video'
          AND v.created_at >= now() - interval '14 days'
        ORDER BY v.views DESC NULLS LAST, v.created_at DESC
        LIMIT 16
        `
      );

      const row = await buildHomeRow({
        req,
        key: "trending",
        title: "Trending",
        dbRows: trendingRows.rows,
        seenIds,
        minVideos: 4,
      });

      if (row) rows.push(row);
    }

    // -------------------------
    // New Uploads
    // -------------------------
    {
      const newRows = await pool.query(
        `
        SELECT
          v.id,
          v.user_id,
          v.title,
          v.description,
          v.category,
          v.visibility,
          v.media_type,
          v.asset_scope,
          v.filename,
          v.thumb,
          v.duration_text,
          v.views,
          v.tags,
          v.created_at AS "createdAt",
          v.updated_at AS "updatedAt",
          u.username AS channel_username,
          COALESCE(p.display_name, '') AS channel_display_name
        FROM videos v
        JOIN users u ON u.id = v.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        WHERE v.visibility = 'public'
          AND v.asset_scope = 'public'
          AND v.media_type = 'video'
        ORDER BY v.created_at DESC
        LIMIT 40
        `
      );

      const row = await buildHomeRow({
        req,
        key: "new-uploads",
        title: "New Uploads",
        dbRows: newRows.rows,
        seenIds,
        minVideos: 1,
        allowDuplicates: true,
        limit: 12,
      });

      if (row) rows.push(row);
    }

    // -------------------------
    // Explore Something New
    // -------------------------
    {
      const params = [];
      let watchedClause = "";

      if (userId) {
        params.push(userId);
        watchedClause = `
          AND NOT EXISTS (
            SELECT 1
            FROM watch_history wh
            WHERE wh.user_id::text = $1::text
              AND wh.video_id::text = v.id::text
          )
        `;
      }

      const exploreRows = await pool.query(
        `
        SELECT
          v.id,
          v.user_id,
          v.title,
          v.description,
          v.category,
          v.visibility,
          v.media_type,
          v.asset_scope,
          v.filename,
          v.thumb,
          v.duration_text,
          v.views,
          v.tags,
          v.created_at AS "createdAt",
          v.updated_at AS "updatedAt",
          u.username AS channel_username,
          COALESCE(p.display_name, '') AS channel_display_name
        FROM videos v
        JOIN users u ON u.id = v.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        WHERE v.visibility = 'public'
          AND v.asset_scope = 'public'
          AND v.media_type = 'video'
          ${watchedClause}
        ORDER BY RANDOM()
        LIMIT 16
        `,
        params
      );

      const row = await buildHomeRow({
        req,
        key: "explore",
        title: "Explore Something New",
        dbRows: exploreRows.rows,
        seenIds,
        minVideos: 4,
      });

      if (row) rows.push(row);
    }

    // -------------------------
    // Final trim
    // -------------------------
    const finalRows = rows.slice(0, 6);

    return res.json(finalRows);
  } catch (e) {
    console.error("GET /api/home error:", e);
    return res.status(500).json({ error: e.message || "Failed to load home rows" });
  }
});

app.patch("/api/videos/:id", requireAuth, async (req, res) => {
  const videoId = String(req.params.id);
  const userId = Number(req.user.id);

  try {
    const existing = await pool.query(
      `
      SELECT id, user_id
      FROM videos
      WHERE id::text = $1::text
      LIMIT 1
      `,
      [videoId]
    );

    const video = existing.rows[0];
    if (!video) {
      return res.status(404).json({ error: "Video not found" });
    }

    if (Number(video.user_id) !== userId) {
      return res.status(403).json({ error: "Not allowed" });
    }

    const rawTitle = String(req.body?.title ?? "").trim();
    const rawDescription = String(req.body?.description ?? "");
    const rawTags = Array.isArray(req.body?.tags) ? req.body.tags : [];

    if (!rawTitle) {
      return res.status(400).json({ error: "Title is required" });
    }

    if (rawTitle.length > 120) {
      return res.status(400).json({ error: "Title is too long" });
    }

    if (rawDescription.length > 5000) {
      return res.status(400).json({ error: "Description is too long" });
    }

    const tags = Array.from(
      new Set(
        rawTags
          .map((t) => String(t || "").trim().toLowerCase())
          .filter(Boolean)
          .slice(0, 30)
      )
    );

    await pool.query(
      `
      UPDATE videos
      SET
        title = $2,
        description = $3,
        tags = $4::text[],
        updated_at = NOW()
      WHERE id::text = $1::text
      `,
      [videoId, rawTitle, rawDescription, tags]
    );

    const fresh = await fetchVideoById(videoId);
    if (!fresh) {
      return res.status(404).json({ error: "Video not found after update" });
    }

    const apiVideo = await toApiVideo(req, fresh);
    return res.json({ ok: true, video: apiVideo });
  } catch (e) {
    console.error("PATCH /api/videos/:id error:", e);
    return res.status(500).json({ error: "Failed to update video" });
  }
});


app.delete("/api/videos/:id", requireAuth, async (req, res) => {
  const videoId = String(req.params.id);
  const userId = Number(req.user.id);

  try {
    // fetch video first (ownership + filenames)
    const vRes = await pool.query(
      `SELECT id, user_id, filename, thumb FROM videos WHERE id::text = $1::text LIMIT 1`,
      [videoId]
    );
    const v = vRes.rows[0];
    if (!v) return res.status(404).json({ error: "Video not found" });
    if (Number(v.user_id) !== userId) return res.status(403).json({ error: "Not allowed" });

    // delete DB row first (or last — either is fine; I prefer DB last if you want strictness)
    await pool.query(`DELETE FROM videos WHERE id::text = $1::text`, [videoId]);

    const VIDEO_SOURCE = process.env.VIDEO_SOURCE || "local";

    // remove storage
    if (VIDEO_SOURCE === "aws") {
      // v.filename is your S3 key (e.g. uploads/<userId>/<file>.mp4)
      if (process.env.S3_UPLOADS_BUCKET && v.filename) {
        await deleteFromS3({ bucket: process.env.S3_UPLOADS_BUCKET, key: v.filename });
      }
      // thumb key in assets bucket (if you store thumbs there)
      if (process.env.S3_ASSETS_BUCKET && v.thumb && v.thumb !== "placeholder.jpg") {
        if (process.env.S3_UPLOADS_BUCKET && v.filename) {
          const bucket = process.env.S3_UPLOADS_BUCKET;

          if (String(v.filename).endsWith("/master.m3u8")) {
            const prefix = String(v.filename).replace(/\/master\.m3u8$/i, "");
            await deletePrefixFromS3({ bucket, prefix: `${prefix}/` });
          } else {
            // fallback (older mp4 uploads)
            await deleteFromS3({ bucket, key: v.filename });
          }
        }
      }
    } else {
      // local cleanup (only if you use VIDEO_SOURCE=local)
      try {
        const filePath = path.join(VIDEO_DIR, v.filename);
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
      } catch {}
      try {
        if (v.thumb && v.thumb !== "placeholder.jpg") {
          const thumbPath = path.join(THUMB_DIR, v.thumb);
          if (fs.existsSync(thumbPath)) fs.unlinkSync(thumbPath);
        }
      } catch {}
    }

    return res.json({ ok: true, id: videoId });
  } catch (e) {
    console.error("DELETE /api/videos/:id error:", e);
    return res.status(500).json({ error: "Failed to delete video" });
  }
});

app.delete("/api/comments/:commentId", requireVerifiedEmail, requireNotBanned, async (req, res) => {
  const commentId = Number(req.params.commentId);
  const userId = Number(req.user.id);

  if (!Number.isFinite(commentId)) {
    return res.status(400).json({ error: "Bad comment id" });
  }

  try {
    // Fetch comment (ownership + video_id)
    const cRes = await pool.query(
      `
      SELECT id, user_id, video_id, parent_comment_id
      FROM video_comments
      WHERE id = $1
      LIMIT 1
      `,
      [commentId]
    );

    const c = cRes.rows[0];
    if (!c) return res.status(404).json({ error: "Comment not found" });

    if (Number(c.user_id) !== userId) {
      return res.status(403).json({ error: "Not allowed" });
    }

    await pool.query("BEGIN");

    // If deleting a top-level comment, delete its replies too (1-level replies system)
    if (c.parent_comment_id == null) {
      // delete likes on replies
      await pool.query(
        `
        DELETE FROM comment_likes
        WHERE comment_id IN (
          SELECT id FROM video_comments WHERE parent_comment_id = $1
        )
        `,
        [commentId]
      );

      // delete replies
      await pool.query(
        `DELETE FROM video_comments WHERE parent_comment_id = $1`,
        [commentId]
      );
    }

    // delete likes on the comment itself
    await pool.query(`DELETE FROM comment_likes WHERE comment_id = $1`, [commentId]);

    // delete the comment
    await pool.query(`DELETE FROM video_comments WHERE id = $1`, [commentId]);

    await pool.query("COMMIT");

    return res.json({ ok: true, commentId });
  } catch (e) {
    await pool.query("ROLLBACK").catch(() => {});
    console.error("DELETE /api/comments/:commentId error:", e);
    return res.status(500).json({ error: "Failed to delete comment" });
  }
});

app.patch("/api/comments/:commentId", requireAuth, async (req, res) => {
  const commentId = Number(req.params.commentId);
  const userId = Number(req.user.id);
  const body = String(req.body?.body || "").trim();

  if (!Number.isFinite(commentId)) {
    return res.status(400).json({ error: "Bad comment id" });
  }
  if (!body) return res.status(400).json({ error: "Comment body required" });
  if (body.length > 2000) return res.status(400).json({ error: "Comment too long" });

  try {
    const existing = await pool.query(
      `SELECT id, user_id FROM video_comments WHERE id = $1 LIMIT 1`,
      [commentId]
    );
    const c = existing.rows[0];
    if (!c) return res.status(404).json({ error: "Comment not found" });

    if (Number(c.user_id) !== userId) {
      return res.status(403).json({ error: "Not allowed" });
    }

    const upd = await pool.query(
      `
      UPDATE video_comments
      SET body = $2, updated_at = now()
      WHERE id = $1
      RETURNING id, body, updated_at, is_shadow_hidden
      `,
      [commentId, body]
    );

    return res.json({
      ok: true,
      comment: {
        id: Number(upd.rows[0].id),
        body: upd.rows[0].body,
        updatedAt: upd.rows[0].updated_at,
        isShadowHidden: !!upd.rows[0].is_shadow_hidden,
      },
    });
  } catch (e) {
    console.error("PATCH /api/comments/:commentId error:", e);
    return res.status(500).json({ error: "Failed to edit comment" });
  }
});



// -------------------------
// COMMENTS API (top-level + one-level replies)
// -------------------------
app.get("/api/videos/:videoId/comments", async (req, res) => {
  try {
    const videoId = req.params.videoId;
    const limit = Math.min(Number(req.query.limit || 50), 200);
    const offset = Math.max(Number(req.query.offset || 0), 0);
    const myUserId = req.user?.id ? Number(req.user.id) : null;

    // top-level comments
    const top = await pool.query(
      `
      SELECT
        c.id,
        c.video_id,
        c.user_id,
        c.body,
        c.created_at,
        c.updated_at,
        c.is_shadow_hidden,
        u.username,
        COALESCE(p.display_name, '') AS display_name,
        COALESCE(cls.like_count, 0) AS like_count,
        CASE
          WHEN $3::bigint IS NULL THEN false
          ELSE EXISTS (
            SELECT 1
            FROM comment_likes cl
            WHERE cl.comment_id = c.id
              AND cl.user_id = $3::bigint
          )
        END AS liked_by_me
      FROM video_comments c
      JOIN users u ON u.id = c.user_id
      LEFT JOIN user_profiles p ON p.user_id = u.id
      LEFT JOIN comment_like_stats cls ON cls.comment_id = c.id
      WHERE c.video_id = $1
        AND c.parent_comment_id IS NULL
        AND (
          c.is_shadow_hidden = FALSE
          OR ($3::bigint IS NOT NULL AND c.user_id = $3::bigint)
        )
      ORDER BY c.created_at DESC
      LIMIT $2 OFFSET $4
      `,
      [videoId, limit, myUserId, offset]
    );

    const topItems = top.rows.map((r) => ({
      id: Number(r.id),
      videoId: r.video_id,
      userId: Number(r.user_id),
      username: r.username,
      displayName: r.display_name || r.username,
      body: r.body,
      createdAt: r.created_at,
      updatedAt: r.updated_at,
      isShadowHidden: !!r.is_shadow_hidden,
      likeCount: Number(r.like_count),
      likedByMe: !!r.liked_by_me,
      replies: [],
    }));

    const parentIds = topItems.map((c) => c.id);

    if (parentIds.length) {
      const replies = await pool.query(
        `
        SELECT
          c.id,
          c.video_id,
          c.user_id,
          c.parent_comment_id,
          c.body,
          c.created_at,
          c.updated_at,
          c.is_shadow_hidden,
          u.username,
          COALESCE(p.display_name, '') AS display_name,
          COALESCE(cls.like_count, 0) AS like_count,
          CASE
            WHEN $3::bigint IS NULL THEN false
            ELSE EXISTS (
              SELECT 1
              FROM comment_likes cl
              WHERE cl.comment_id = c.id
                AND cl.user_id = $3::bigint
            )
          END AS liked_by_me
        FROM video_comments c
        JOIN users u ON u.id = c.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        LEFT JOIN comment_like_stats cls ON cls.comment_id = c.id
        WHERE c.video_id = $1
          AND c.parent_comment_id = ANY($2::bigint[])
          AND (
            c.is_shadow_hidden = FALSE
            OR ($3::bigint IS NOT NULL AND c.user_id = $3::bigint)
          )
        ORDER BY c.created_at ASC
        `,
        [videoId, parentIds, myUserId]
      );

      const byParent = new Map();

      for (const r of replies.rows) {
        const pid = Number(r.parent_comment_id);
        if (!byParent.has(pid)) byParent.set(pid, []);
        byParent.get(pid).push({
          id: Number(r.id),
          videoId: r.video_id,
          userId: Number(r.user_id),
          username: r.username,
          displayName: r.display_name || r.username,
          body: r.body,
          createdAt: r.created_at,
          updatedAt: r.updated_at,
          isShadowHidden: !!r.is_shadow_hidden,
          likeCount: Number(r.like_count),
          likedByMe: !!r.liked_by_me,
        });
      }

      for (const c of topItems) {
        c.replies = byParent.get(c.id) || [];
      }
    }

    res.json({ videoId, items: topItems, limit, offset });
  } catch (e) {
    console.error("GET /api/videos/:videoId/comments error:", e);
    res.status(500).json({ error: "Failed to load comments" });
  }
});

app.post("/api/videos/:videoId/comments", requireVerifiedEmail, requireNotBanned, async (req, res) => {
  try {
    const videoId = req.params.videoId;
    const userId = Number(req.user.id);

    const body = String(req.body?.body || "").trim();
    const parentCommentId = req.body?.parentCommentId ?? null;

    if (!body) return res.status(400).json({ error: "Comment body required" });
    if (body.length > 2000) return res.status(400).json({ error: "Comment too long" });

    // load user's shadow-ban state
    const userRes = await pool.query(
      `
      SELECT is_comment_shadowbanned
      FROM users
      WHERE id = $1
      LIMIT 1
      `,
      [userId]
    );

    const isShadowHidden = !!userRes.rows[0]?.is_comment_shadowbanned;

    // validate parent if provided (must be top-level comment on same video)
    let parentId = null;
    if (parentCommentId !== null && parentCommentId !== undefined && parentCommentId !== "") {
      const pid = Number(parentCommentId);
      if (!Number.isFinite(pid)) {
        return res.status(400).json({ error: "Bad parentCommentId" });
      }

      const parent = await pool.query(
        `
        SELECT id
        FROM video_comments
        WHERE id = $1
          AND video_id = $2
          AND parent_comment_id IS NULL
        `,
        [pid, videoId]
      );

      if (!parent.rows.length) {
        return res
          .status(400)
          .json({ error: "Parent comment not found (or not top-level)" });
      }

      parentId = pid;
    }

    const result = await pool.query(
      `
      INSERT INTO video_comments (
        video_id,
        user_id,
        body,
        parent_comment_id,
        is_shadow_hidden
      )
      VALUES ($1, $2, $3, $4, $5)
      RETURNING
        id,
        video_id,
        user_id,
        body,
        parent_comment_id,
        is_shadow_hidden,
        created_at,
        updated_at
      `,
      [videoId, userId, body, parentId, isShadowHidden]
    );

    const profile = await pool.query(
      `SELECT COALESCE(display_name, '') AS display_name FROM user_profiles WHERE user_id = $1`,
      [userId]
    );

    const displayName = profile.rows[0]?.display_name || req.user.username;

    res.json({
      ok: true,
      comment: {
        id: Number(result.rows[0].id),
        videoId: result.rows[0].video_id,
        userId: Number(result.rows[0].user_id),
        username: req.user.username,
        displayName,
        body: result.rows[0].body,
        parentCommentId: result.rows[0].parent_comment_id
          ? Number(result.rows[0].parent_comment_id)
          : null,
        isShadowHidden: !!result.rows[0].is_shadow_hidden,
        createdAt: result.rows[0].created_at,
        updatedAt: result.rows[0].updated_at,
        likeCount: 0,
        likedByMe: false,
        replies: [],
      },
    });
  } catch (e) {
    console.error("POST /api/videos/:videoId/comments error:", e);
    res.status(500).json({ error: "Failed to post comment" });
  }
});

app.post("/api/comments/:commentId/toggle-like", requireVerifiedEmail, requireNotBanned, async (req, res) => {
  const commentId = Number(req.params.commentId);
  const userId = req.user.id;

  if (!Number.isFinite(commentId)) return res.status(400).json({ error: "Bad comment id" });

  try {
    const existing = await pool.query(
      `SELECT 1 FROM comment_likes WHERE comment_id = $1 AND user_id = $2`,
      [commentId, userId]
    );

    if (existing.rows.length) {
      await pool.query(
        `DELETE FROM comment_likes WHERE comment_id = $1 AND user_id = $2`,
        [commentId, userId]
      );
    } else {
      await pool.query(
        `INSERT INTO comment_likes (comment_id, user_id) VALUES ($1, $2)`,
        [commentId, userId]
      );
    }

    const stats = await pool.query(
      `SELECT COUNT(*)::int AS like_count FROM comment_likes WHERE comment_id = $1`,
      [commentId]
    );

    res.json({
      ok: true,
      commentId,
      liked: !existing.rows.length,
      likeCount: stats.rows[0].like_count,
    });
  } catch (e) {
    console.error("POST /api/comments/:commentId/toggle-like error:", e);
    res.status(500).json({ error: "Failed to toggle like" });
  }
});

app.post("/api/videos/:id/view", requireAuth, async (req, res) => {
  const videoId = String(req.params.id);
  const userId = Number(req.user.id);

  try {
    const result = await pool.query(
      `
      WITH ins AS (
        INSERT INTO video_views (video_id, user_id)
        VALUES ($1::text, $2::bigint)
        ON CONFLICT (video_id, user_id) DO NOTHING
        RETURNING 1
      ),
      upd AS (
        UPDATE videos
        SET views = views + (SELECT COUNT(*) FROM ins)
        WHERE id = $1::text
        RETURNING views
      )
      SELECT
        (SELECT views FROM upd) AS views,
        (SELECT COUNT(*) FROM ins)::int AS added;
      `,
      [videoId, userId]
    );

    if (!result.rows.length || result.rows[0].views == null) {
      return res.status(404).json({ error: "Video not found" });
    }

    res.json({
      ok: true,
      videoId,
      views: Number(result.rows[0].views),
      added: Number(result.rows[0].added || 0),
    });
  } catch (e) {
    console.error("POST /api/videos/:id/view error:", e);
    res.status(500).json({ error: "Failed to record view" });
  }
});



// -------------------------
// Ratings API
// -------------------------
app.get("/api/videos/:id/my-rating", requireAuth, async (req, res) => {
  const videoId = String(req.params.id);
  const userId = String(req.user.id);

  try {
    const { rows } = await pool.query(
      `SELECT rating FROM video_ratings WHERE video_id = $1 AND user_id = $2`,
      [videoId, userId]
    );

    res.json({ rating: rows[0]?.rating ?? null });
  } catch (err) {
    console.error("my-rating error:", err);
    res.status(500).json({ error: "Failed to fetch my rating" });
  }
});

app.post("/api/videos/:id/rate", requireVerifiedEmail, requireNotBanned, async (req, res) => {
  const videoId = String(req.params.id);
  const userId = String(req.user.id);
  const rating = Number(req.body.rating);

  if (!Number.isInteger(rating) || rating < 1 || rating > 5) {
    return res.status(400).json({ error: "Rating must be 1-5" });
  }

  try {
    await pool.query(
      `
      INSERT INTO video_ratings (video_id, user_id, rating)
      VALUES ($1, $2, $3)
      ON CONFLICT (video_id, user_id)
      DO UPDATE SET rating = EXCLUDED.rating, updated_at = now()
      `,
      [videoId, userId, rating]
    );

    // 1) Who owns this video?
    const ownerRes = await pool.query(
      `SELECT user_id FROM videos WHERE id::text = $1::text LIMIT 1`,
      [videoId]
    );
    const ownerId = ownerRes.rows[0]?.user_id;
    if (!ownerId) return res.status(404).json({ error: "Video not found" });

    // 2) Recompute owner's overall rating across ALL ratings on ALL their videos
    const ownerAgg = await pool.query(
      `
      SELECT
        COALESCE(AVG(vr.rating)::float, 0) AS avg,
        COUNT(*)::int AS count
      FROM video_ratings vr
      JOIN videos v ON v.id = vr.video_id
      WHERE v.user_id = $1
      `,
      [ownerId]
    );

    // 3) Persist to users table so /api/profile/u/:username stays correct
    await pool.query(
      `
      UPDATE users
      SET rating = $2::float,
          review_count = $3::int
      WHERE id = $1
      `,
      [ownerId, ownerAgg.rows[0].avg, ownerAgg.rows[0].count]
    );


    const agg = await pool.query(
      `SELECT AVG(rating)::float AS avg, COUNT(*)::int AS count
       FROM video_ratings WHERE video_id = $1`,
      [videoId]
    );

    res.json({
      ratingAvg: agg.rows[0]?.avg ?? 0,
      ratingCount: agg.rows[0]?.count ?? 0,
    });
  } catch (err) {
    console.error("rate error:", err);
    res.status(500).json({ error: "Failed to rate video" });
  }
});

app.post("/api/beta-signup", async (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const watching = !!req.body?.watching;
    const creating = !!req.body?.creating;

    if (!email) {
      return res.status(400).json({ error: "Email is required" });
    }

    const emailOk = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
    if (!emailOk) {
      return res.status(400).json({ error: "Please enter a valid email address" });
    }

    const existing = await pool.query(
      `SELECT id FROM beta_waitlist WHERE email = $1 LIMIT 1`,
      [email]
    );

    if (existing.rows.length) {
      return res.json({
        ok: true,
        alreadySignedUp: true,
        message: "You're already on the waitlist.",
      });
    }

    await pool.query(
      `
      INSERT INTO beta_waitlist (email, watching, creating)
      VALUES ($1, $2, $3)
      `,
      [email, watching, creating]
    );

    return res.json({
      ok: true,
      alreadySignedUp: false,
      message: "Thanks! You've joined the beta waitlist.",
    });
  } catch (e) {
    console.error("POST /api/beta-signup error:", e);
    return res.status(500).json({ error: "Failed to join waitlist" });
  }
});


app.post("/api/videos/:id/history", requireVerifiedEmail, async (req, res) => {
  const videoId = String(req.params.id);
  const userId = Number(req.user.id);
  const progressSeconds = Number(req.body?.progressSeconds || 0);

  try {
    const exists = await pool.query(
      `SELECT id FROM videos WHERE id::text = $1::text LIMIT 1`,
      [videoId]
    );

    if (!exists.rows.length) {
      return res.status(404).json({ error: "Video not found" });
    }

    await pool.query(
      `
      INSERT INTO watch_history (user_id, video_id, watched_at, progress_seconds)
      VALUES ($1, $2, NOW(), $3)
      ON CONFLICT (user_id, video_id)
      DO UPDATE SET
        watched_at = NOW(),
        progress_seconds = EXCLUDED.progress_seconds
      `,
      [userId, videoId, Math.max(0, progressSeconds)]
    );

    return res.json({ ok: true, videoId });
  } catch (e) {
    console.error("POST /api/videos/:id/history error:", e);
    return res.status(500).json({ error: "Failed to record history" });
  }
});


app.delete("/api/me/history/:videoId", requireAuth, async (req, res) => {
  const userId = Number(req.user.id);
  const videoId = String(req.params.videoId);

  try {
    await pool.query(
      `
      DELETE FROM watch_history
      WHERE user_id = $1 AND video_id = $2
      `,
      [userId, videoId]
    );

    return res.json({ ok: true, videoId });
  } catch (e) {
    console.error("DELETE /api/me/history/:videoId error:", e);
    return res.status(500).json({ error: "Failed to remove history item" });
  }
});


app.delete("/api/me/history", requireAuth, async (req, res) => {
  const userId = Number(req.user.id);

  try {
    await pool.query(`DELETE FROM watch_history WHERE user_id = $1`, [userId]);
    return res.json({ ok: true });
  } catch (e) {
    console.error("DELETE /api/me/history error:", e);
    return res.status(500).json({ error: "Failed to clear history" });
  }
});


app.post("/api/videos/:id/report", requireAuth, async (req, res) => {
  const videoId = String(req.params.id || "");
  const offense = String(req.body?.offense || "").trim();
  const comments = String(req.body?.comments || "").trim();

  const allowedOffenses = new Set([
    "Spam or misleading",
    "Harassment or bullying",
    "Hateful or abusive content",
    "Violence or dangerous acts",
    "Sexual content",
    "Copyright or stolen content",
    "Other",
  ]);

  if (!videoId) {
    return res.status(400).json({ error: "Missing video id" });
  }

  if (!allowedOffenses.has(offense)) {
    return res.status(400).json({ error: "Invalid report reason" });
  }

  if (comments.length > 2000) {
    return res.status(400).json({ error: "Comments are too long" });
  }

  try {
    const exists = await pool.query(
      `SELECT id FROM videos WHERE id::text = $1::text LIMIT 1`,
      [videoId]
    );

    if (!exists.rows.length) {
      return res.status(404).json({ error: "Video not found" });
    }

    await pool.query(
      `
      INSERT INTO video_reports (video_id, reporter_id, offense, comments)
      VALUES ($1, $2, $3, $4)
      `,
      [videoId, req.user.id, offense, comments]
    );

    return res.json({ ok: true });
  } catch (e) {
    console.error("POST /api/videos/:id/report error:", e);
    return res.status(500).json({ error: "Failed to submit report" });
  }
});


// -------------------------
// Videos API
// -------------------------
app.get("/api/videos", async (req, res) => {
  try {
    const q = String(req.query.q || "").trim();
    const category = String(req.query.category || "").trim();
    const sort = String(req.query.sort || "newest").toLowerCase().trim();
    const filter = String(req.query.filter || "").toLowerCase().trim();

    const requesterId = req.user?.id ? Number(req.user.id) : null;

        // -------------------------
    // Watch Later mode
    // -------------------------
    if (filter === "watch-later") {
      if (!requesterId) {
        return res.status(401).json({ error: "Not logged in" });
      }

      const watchLaterResult = await pool.query(
        `
        SELECT
          v.id,
          v.user_id,
          v.title,
          v.description,
          v.category,
          v.visibility,
          v.media_type,
          v.asset_scope,
          v.filename,
          v.thumb,
          v.duration_text,
          v.duration,
          v.views,
          v.tags,
          v.created_at AS "createdAt",
          v.updated_at AS "updatedAt",
          u.username AS channel_username,
          COALESCE(p.display_name, '') AS channel_display_name,
          wl.created_at AS saved_at
        FROM watch_later wl
        JOIN videos v ON v.id::text = wl.video_id::text
        JOIN users u ON u.id = v.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        WHERE wl.user_id = $1
          AND v.visibility = 'public'
          AND v.asset_scope = 'public'
          AND v.media_type = 'video'
        ORDER BY wl.created_at DESC
        LIMIT 200
        `,
        [requesterId]
      );

      const enriched = await Promise.all(
        watchLaterResult.rows.map(async (v) => {
          const apiVideo = await toApiVideo(req, v);
          return {
            ...apiVideo,
            savedAt: v.saved_at || null,
          };
        })
      );

      return res.json(enriched);
    }


        // -------------------------
    // Watch History mode
    // -------------------------
    if (filter === "history") {
      if (!requesterId) {
        return res.status(401).json({ error: "Not logged in" });
      }

      const historyResult = await pool.query(
        `
        SELECT
          v.id,
          v.user_id,
          v.title,
          v.description,
          v.category,
          v.visibility,
          v.media_type,
          v.asset_scope,
          v.filename,
          v.thumb,
          v.duration_text,
          v.views,
          v.tags,
          v.created_at AS "createdAt",
          v.updated_at AS "updatedAt",
          u.username AS channel_username,
          COALESCE(p.display_name, '') AS channel_display_name,
          wh.watched_at,
          wh.progress_seconds
        FROM watch_history wh
        JOIN videos v ON v.id::text = wh.video_id::text
        JOIN users u ON u.id = v.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        WHERE wh.user_id = $1
          AND v.visibility = 'public'
          AND v.asset_scope = 'public'
          AND v.media_type = 'video'
        ORDER BY wh.watched_at DESC
        LIMIT 200
        `,
        [requesterId]
      );

      const enriched = await Promise.all(
        historyResult.rows.map(async (v) => {
          const apiVideo = await toApiVideo(req, v);
          return {
            ...apiVideo,
            watchedAt: v.watched_at || null,
            progressSeconds: Number(v.progress_seconds || 0),
          };
        })
      );

      return res.json(enriched);
    }

    // -------------------------
    // Recently Rated mode
    // -------------------------
    if (filter === "rated") {
      if (!requesterId) {
        return res.status(401).json({ error: "Not logged in" });
      }

      const ratedResult = await pool.query(
        `
        SELECT
          v.id,
          v.user_id,
          v.title,
          v.description,
          v.category,
          v.visibility,
          v.media_type,
          v.asset_scope,
          v.filename,
          v.thumb,
          v.duration_text,
          v.views,
          v.tags,
          v.created_at AS "createdAt",
          v.updated_at AS "updatedAt",
          u.username AS channel_username,
          COALESCE(p.display_name, '') AS channel_display_name,
          vr.rating AS my_rating,
          vr.updated_at AS rated_at
        FROM video_ratings vr
        JOIN videos v ON v.id::text = vr.video_id::text
        JOIN users u ON u.id = v.user_id
        LEFT JOIN user_profiles p ON p.user_id = u.id
        WHERE vr.user_id = $1
          AND v.visibility = 'public'
          AND v.asset_scope = 'public'
          AND v.media_type = 'video'
        ORDER BY vr.updated_at DESC
        LIMIT 200
        `,
        [requesterId]
      );

      const enriched = await Promise.all(
        ratedResult.rows.map(async (v) => {
          const apiVideo = await toApiVideo(req, v);
          return {
            ...apiVideo,
            myRating: Number(v.my_rating || 0),
            ratedAt: v.rated_at || null,
          };
        })
      );

      return res.json(enriched);
    }

    // -------------------------
    // Normal browse / search mode
    // -------------------------
    let orderBy = "v.created_at DESC";
    if (sort === "oldest") orderBy = "v.created_at ASC";
    else if (sort === "views") orderBy = "v.views DESC NULLS LAST, v.created_at DESC";
    else if (sort === "highest") {
      orderBy =
        "COALESCE(vrs.rating_avg, 0) DESC, COALESCE(vrs.rating_count, 0) DESC, v.created_at DESC";
    }

    const where = [
      `v.visibility = 'public'`,
      `v.asset_scope = 'public'`,
      `v.media_type = 'video'`,
    ];

    const params = [];
    let i = 1;

    if (category) {
      where.push(`v.category = $${i++}`);
      params.push(category);
    }

    if (q) {
      const tokens = q.split(/\s+/).filter(Boolean).slice(0, 10);
      for (const t of tokens) {
        where.push(`
          (
            v.title ILIKE $${i}
            OR v.description ILIKE $${i}
            OR v.category ILIKE $${i}
            OR EXISTS (
              SELECT 1
              FROM unnest(COALESCE(v.tags, ARRAY[]::text[])) tag
              WHERE tag ILIKE $${i}
            )
            OR u.username ILIKE $${i}
            OR COALESCE(p.display_name,'') ILIKE $${i}
          )
        `);
        params.push(`%${t}%`);
        i++;
      }
    }

    const result = await pool.query(
      `
      SELECT
        v.id,
        v.user_id,
        v.title,
        v.description,
        v.category,
        v.visibility,
        v.media_type,
        v.asset_scope,
        v.filename,
        v.thumb,
        v.duration_text,
        v.views,
        v.tags,
        v.created_at AS "createdAt",
        v.updated_at AS "updatedAt",
        u.username AS channel_username,
        COALESCE(p.display_name, '') AS channel_display_name
      FROM videos v
      JOIN users u ON u.id = v.user_id
      LEFT JOIN user_profiles p ON p.user_id = u.id
      LEFT JOIN video_rating_stats vrs ON vrs.video_id = v.id
      WHERE ${where.join(" AND ")}
      ORDER BY ${orderBy}
      LIMIT 200
      `,
      params
    );

    const enriched = await Promise.all(result.rows.map((v) => toApiVideo(req, v)));
    res.json(enriched);
  } catch (e) {
    console.error("GET /api/videos search error:", e);
    res.status(500).json({ error: "Failed to load videos" });
  }
});


app.get("/api/videos/:id", async (req, res) => {
  try {
    const v = await fetchVideoById(req.params.id);
    if (!v) return res.status(404).json({ error: "Not found" });

    const requesterId = req.user?.id ? Number(req.user.id) : null;
    const ownerId = Number(v.user_id);

    const isOwner = requesterId != null && requesterId === ownerId;

    // Library assets are owner-only
    if (v.asset_scope === "library" && !isOwner) {
      return res.status(404).json({ error: "Not found" });
    }

    // Private/unlisted are owner-only
    if (v.visibility !== "public" && !isOwner) {
      return res.status(404).json({ error: "Not found" });
    }

    const apiVideo = await toApiVideo(req, v);

    let progressSeconds = 0;
    let watchedAt = null;

    if (requesterId != null) {
      const historyRes = await pool.query(
        `
        SELECT progress_seconds, watched_at
        FROM watch_history
        WHERE user_id = $1
          AND video_id::text = $2::text
        LIMIT 1
        `,
        [requesterId, String(v.id)]
      );

      if (historyRes.rows[0]) {
        progressSeconds = Number(historyRes.rows[0].progress_seconds || 0);
        watchedAt = historyRes.rows[0].watched_at || null;
      }
    }

    res.json({
      ...apiVideo,
      progressSeconds,
      watchedAt,
    });
  } catch (e) {
    console.error("GET /api/videos/:id error:", e);
    res.status(500).json({ error: "Failed to load video" });
  }
});

// -------------------------
// Upload video
// -------------------------
app.post("/api/videos/upload", requireAuth, upload.single("video"), async (req, res) => {
  const uploadId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const t0 = Date.now();

  const log = (msg, extra) => {
    const ms = Date.now() - t0;
    if (extra !== undefined) console.log(`[upload ${uploadId}] +${ms}ms ${msg}`, extra);
    else console.log(`[upload ${uploadId}] +${ms}ms ${msg}`);
  };

  try {
    log("START /api/videos/upload");

    const userId = req.user.id;

    log("req.user", { userId });
    log("req.body keys", Object.keys(req.body || {}));
    log(
      "req.file",
      req.file
        ? {
            originalname: req.file.originalname,
            mimetype: req.file.mimetype,
            sizeBytes: req.file.size,
            filename: req.file.filename,
            path: req.file.path,
          }
        : null
    );

    const title = String(req.body?.title || "").trim();
    const description = String(req.body?.description || "").trim();
    const visibility = String(req.body?.visibility || "public").toLowerCase();
    const tagsRaw = String(req.body?.tags || "");

    let mediaType = String(req.body?.mediaType || "").toLowerCase().trim(); // don't default yet
    const assetScope = String(req.body?.assetScope || "public").toLowerCase().trim();

    // ✅ infer mediaType from the uploaded file if not provided (or wrong)
    const mime = String(req.file?.mimetype || "");
    const ext = path.extname(req.file?.originalname || "").toLowerCase();

    const looksAudio =
      mime.startsWith("audio/") || [".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".opus"].includes(ext);
    const looksVideo =
      mime.startsWith("video/") || [".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v", ".mpeg", ".mpg"].includes(ext);

    if (!mediaType) {
      mediaType = looksAudio ? "audio" : "video"; // fallback
    } else {
      // if user says "video" but it's clearly audio, correct it
      if (mediaType === "video" && looksAudio && !looksVideo) mediaType = "audio";
      if (mediaType === "audio" && looksVideo && !looksAudio) mediaType = "video";
    }

    log("mediaType resolved", { mediaType, mimetype: req.file?.mimetype, originalname: req.file?.originalname });

    const allowedMedia = new Set(["video", "audio"]);
    const allowedScope = new Set(["public", "library"]);

    if (!allowedMedia.has(mediaType)) {
      log("FAIL validation: bad mediaType", { mediaType });
      return res.status(400).json({ error: "mediaType must be video or audio" });
    }
    if (!allowedScope.has(assetScope)) {
      log("FAIL validation: bad assetScope", { assetScope });
      return res.status(400).json({ error: "assetScope must be public or library" });
    }

    // force private if library
    const effectiveVisibility = assetScope === "library" ? "private" : visibility;


    if (!title) {
      log("FAIL validation: missing title");
      return res.status(400).json({ error: "Title is required" });
    }
    if (!req.file) {
      log("FAIL validation: missing file");
      return res.status(400).json({ error: "MP4 file is required" });
    }

    const allowedVis = new Set(["public", "private", "unlisted"]);
    if (!allowedVis.has(visibility)) {
      log("FAIL validation: bad visibility", { visibility });
      return res
        .status(400)
        .json({ error: "Visibility must be public, private, or unlisted" });
    }

    const tags = Array.from(
      new Set(
        tagsRaw
          .split(",")
          .map((t) => t.trim().toLowerCase())
          .filter(Boolean)
          .slice(0, 30)
      )
    );

    const category = "Other";

    // ---------- Thumbnail ----------
    const base = path.parse(req.file.filename).name;
    const thumbName = `${base}.jpg`;
    const thumbPath = path.join(THUMB_DIR, thumbName);

    let storedThumb = "placeholder.jpg";

    if (mediaType === "video") {
      const base = path.parse(req.file.filename).name;
      const thumbName = `${base}.jpg`;
      const thumbPath = path.join(THUMB_DIR, thumbName);

      log("THUMB start", { thumbName, thumbPath });
      const tThumb = Date.now();

      try {
        await generateThumbnailHalfwayWithFallback(req.file.path, thumbPath);
        storedThumb = thumbName;
        log("THUMB ok", { ms: Date.now() - tThumb, storedThumb });
      } catch (e) {
        log("THUMB failed, using placeholder", { ms: Date.now() - tThumb, error: e?.message });
        try { if (fs.existsSync(thumbPath)) fs.unlinkSync(thumbPath); } catch {}
      }
    } else {
      log("THUMB skipped (audio)");
    }

    // What we store in DB (local filename or S3 key)
    let storedFilename = req.file.filename;


    // ---------- AWS pipeline (HLS) ----------
  if (VIDEO_SOURCE === "aws") {
  if (!process.env.S3_UPLOADS_BUCKET) {
    throw new Error("Missing env S3_UPLOADS_BUCKET while VIDEO_SOURCE=aws");
  }

  const bucket = process.env.S3_UPLOADS_BUCKET;

  if (mediaType === "video") {
    const base = path.parse(req.file.filename).name;
    const hlsOutDir = path.join(DATA_ROOT, "hls", `${userId}`, base);

    log("HLS transcode start", { input: req.file.path, outDir: hlsOutDir });
    const tHls = Date.now();
    await generateHls(req.file.path, hlsOutDir);
    log("HLS transcode ok", { ms: Date.now() - tHls });

    const hlsKeyPrefix = `hls/${userId}/${base}`;
    log("S3 HLS upload start", { bucket, keyPrefix: hlsKeyPrefix });

    const tS3 = Date.now();
    await retry(() =>
      uploadDirToS3({
        uploadFileToS3,
        bucket: uploadsBucket,
        localDir: hlsDir,
        keyPrefix: hlsKeyPrefix,
      })
    );
    log("S3 HLS upload ok", { ms: Date.now() - tS3 });

    storedFilename = `${hlsKeyPrefix}/master.m3u8`;

    // cleanup hlsOutDir
    try { if (fs.existsSync(hlsOutDir)) fs.rmSync(hlsOutDir, { recursive: true, force: true }); } catch {}
  } else {
    // AUDIO: upload raw file (no HLS yet)
    const audioKey = `uploads/${userId}/${req.file.filename}`;
    log("S3 audio upload start", { bucket, key: audioKey });

    const tS3Audio = Date.now();
    await uploadFileToS3({
      bucket,
      key: audioKey,
      filePath: req.file.path,
      contentType: req.file.mimetype || "application/octet-stream",
    });
    log("S3 audio upload ok", { ms: Date.now() - tS3Audio });

    storedFilename = audioKey;
  }

   // Upload thumb to assets bucket (only if we generated one)
  if (storedThumb !== "placeholder.jpg") {
    const assetsBucket = process.env.S3_ASSETS_BUCKET;
    if (!assetsBucket) {
      throw new Error("Missing env S3_ASSETS_BUCKET while uploading thumbnail");
    }

    const thumbPath = path.join(THUMB_DIR, storedThumb);
    if (fs.existsSync(thumbPath)) {
      const assetsS3 = makeAssetsS3Client();
      const thumbKey = `thumbs/${userId}/${storedThumb}`;

      log("S3 thumb upload start", {
        bucket: assetsBucket,
        key: thumbKey,
      });

      await retry(() =>
        uploadFileToAssetsBucket({
          assetsS3,
          bucket: assetsBucket,
          key: thumbKey,
          filePath: thumbPath,
          contentType: "image/jpeg",
        })
      );

      storedThumb = thumbKey;
      log("S3 thumb upload ok", { key: storedThumb });

      try {
        fs.unlinkSync(thumbPath);
      } catch {}
    } else {
      throw new Error(`Generated thumbnail missing at expected path: ${thumbPath}`);
    }
  }

  // Cleanup local uploaded file
  try { if (fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path); } catch {}
}

    // ---------- DB insert ----------
    log("DB insert start", { storedFilename, storedThumb, visibility: effectiveVisibility, mediaType, assetScope, tagsCount: tags.length });
    const tDb = Date.now();

    const ins = await pool.query(
      `
      INSERT INTO videos (
        user_id, title, description, category, visibility,
        media_type, asset_scope,
        filename, thumb, duration_text, views, tags
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 0, $11)
      RETURNING id
      `,
      [
        userId,
        title,
        description,
        category,
        effectiveVisibility,
        mediaType,
        assetScope,
        storedFilename,
        storedThumb,
        null,
        tags,
      ]
    );

    const insertedId = ins.rows[0].id;
    log("DB insert ok", { ms: Date.now() - tDb, insertedId });

    // ---------- Response build ----------
    log("Fetch inserted video + build response start");
    const tResp = Date.now();
    const v = await fetchVideoById(insertedId);
    const apiVideo = await toApiVideo(req, v);
    log("Response build ok", { ms: Date.now() - tResp });

    log("DONE ok", { totalMs: Date.now() - t0 });
    return res.json({ ok: true, video: apiVideo });
  } catch (e) {
    log("Upload error", { error: e?.message, stack: e?.stack });

    if (req.file?.path) {
      log("Attempt cleanup of req.file.path", { path: req.file.path });
      try {
        fs.unlinkSync(req.file.path);
        log("Cleanup req.file.path ok");
      } catch (cleanupErr) {
        log("Cleanup req.file.path failed", { error: cleanupErr?.message });
      }
    }

    log("DONE error", { totalMs: Date.now() - t0 });
    return res.status(500).json({
      error: e?.message || "Failed to upload video",
    });
  }
});



// -------------------------
// Static thumbs (local placeholder + local mode thumbs)
// -------------------------
app.use("/thumbs", express.static(THUMB_DIR));

// -------------------------
// Local streaming endpoint (only used when VIDEO_SOURCE=local)
// -------------------------
app.get("/videos/:id/stream", async (req, res) => {
  try {
    // If you're not using local streaming, bail early.
    if (VIDEO_SOURCE !== "local") {
      return res.status(404).json({ error: "Streaming endpoint not used in this mode" });
    }

    const videoId = String(req.params.id || "");
    const v = await fetchVideoById(videoId);
    if (!v) return res.status(404).end("Not found");

    // Permissions (library/private/unlisted are owner-only)
    const requesterId = req.user?.id != null ? Number(req.user.id) : null;
    const ownerId = Number(v.user_id);
    const isOwner = requesterId != null && requesterId === ownerId;

    if (v.asset_scope === "library" && !isOwner) return res.status(404).end("Not found");
    if (v.visibility !== "public" && !isOwner) return res.status(404).end("Not found");

    const filePath = path.join(VIDEO_DIR, v.filename);
    if (!fs.existsSync(filePath)) return res.status(404).end("Missing file");

    const stat = fs.statSync(filePath);
    const fileSize = stat.size;
    const range = req.headers.range;

    const ext = path.extname(filePath).toLowerCase();
    const contentType =
      ext === ".mp4" ? "video/mp4"
      : ext === ".mp3" ? "audio/mpeg"
      : ext === ".wav" ? "audio/wav"
      : ext === ".m4a" ? "audio/mp4"
      : "application/octet-stream";

    if (!range) {
      res.writeHead(200, {
        "Content-Length": fileSize,
        "Content-Type": contentType,
      });
      fs.createReadStream(filePath).pipe(res);
      return;
    }

    const parts = range.replace(/bytes=/, "").split("-");
    const start = parseInt(parts[0], 10);
    const end = parts[1] ? parseInt(parts[1], 10) : fileSize - 1;

    if (start >= fileSize) {
      res.status(416).set("Content-Range", `bytes */${fileSize}`).end();
      return;
    }

    const chunkSize = end - start + 1;
    res.writeHead(206, {
      "Content-Range": `bytes ${start}-${end}/${fileSize}`,
      "Accept-Ranges": "bytes",
      "Content-Length": chunkSize,
      "Content-Type": contentType,
    });

    fs.createReadStream(filePath, { start, end }).pipe(res);
  } catch (e) {
    console.error("GET /videos/:id/stream error:", e);
    res.status(500).json({ error: "Stream failed" });
  }
});

// -------------------------
// Debug
// -------------------------
app.get("/__whoami", (req, res) => {
  res.json({ ok: true, user: req.user ?? null, time: new Date().toISOString() });
});

app.get("/", (_req, res) => {
  res.send("MYTUBE server ✅ Try /api/videos");
});

app.use((err, _req, res, _next) => {
  // Multer errors & our fileFilter errors land here
  if (err) {
    const msg = err.message || "Upload failed";
    // Treat upload validation problems as 400
    const status = msg.toLowerCase().includes("video") || msg.toLowerCase().includes("file")
      ? 400
      : 500;
    return res.status(status).json({ error: msg });
  }
  return res.status(500).json({ error: "Unknown error" });
});


const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
