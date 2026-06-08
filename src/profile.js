// profile.js
import express from "express";
import { pool } from "./db.js";

const router = express.Router();

function requireAuth(req, res, next) {
  if (!req.user) return res.status(401).json({ error: "Not logged in" });
  next();
}

function baseUrl(req) {
  return `${req.protocol}://${req.get("host")}`;
}

function buildPlaybackUrl(req, filename) {
  const VIDEO_SOURCE = process.env.VIDEO_SOURCE || "local";
  const CDN_UPLOADS_BASE_URL = (process.env.CDN_UPLOADS_BASE_URL || "").replace(/\/$/, "");

  if (VIDEO_SOURCE === "aws") {
    if (CDN_UPLOADS_BASE_URL) return `${CDN_UPLOADS_BASE_URL}/${filename}`;
    if (process.env.S3_UPLOADS_BUCKET && process.env.AWS_REGION) {
      return `https://${process.env.S3_UPLOADS_BUCKET}.s3.${process.env.AWS_REGION}.amazonaws.com/${filename}`;
    }
  }

  // local fallback (server.js stream endpoint)
  // NOTE: profile page uses /videos/:id/stream style
  return `${baseUrl(req)}/videos/__ID__/stream`; // replaced per-video below
}

function buildThumbUrl(req, thumb) {
  const VIDEO_SOURCE = process.env.VIDEO_SOURCE || "local";
  const CDN_ASSETS_BASE_URL = (process.env.CDN_ASSETS_BASE_URL || "").replace(/\/$/, "");

  if (VIDEO_SOURCE === "aws" && thumb && thumb !== "placeholder.jpg" && CDN_ASSETS_BASE_URL) {
    return `${CDN_ASSETS_BASE_URL}/${thumb}`;
  }

  const b = baseUrl(req);
  return thumb ? `${b}/thumbs/${thumb}` : `${b}/thumbs/placeholder.jpg`;
}


// -------------------------
// Public profile by username
// GET /api/profile/u/:username
// -------------------------
router.get("/u/:username", async (req, res) => {
  const username = String(req.params.username || "").trim();
  if (!username) return res.status(400).json({ error: "Missing username" });

  const result = await pool.query(
    `
    SELECT
      u.id,
      u.username,
      u.tokens,
      u.rating,
      u.review_count,
      p.display_name,
      p.bio,
      p.avatar_url,
      p.banner_url,
      p.location,
      p.website
    FROM users u
    LEFT JOIN user_profiles p ON p.user_id = u.id
    WHERE lower(u.username) = lower($1)
    LIMIT 1
    `,
    [username]
  );

  if (!result.rows[0]) return res.status(404).json({ error: "User not found" });

  const r = result.rows[0];
  res.json({
    id: Number(r.id),
    username: r.username,
    tokens: r.tokens,
    rating: r.rating,
    reviewCount: r.review_count,
    displayName: r.display_name || r.username,
    bio: r.bio || "",
    avatarUrl: r.avatar_url || "",
    bannerUrl: r.banner_url || "",
    location: r.location || "",
    website: r.website || "",
  });
});


// -------------------------
// My profile (edit preload)
// GET /api/profile/me
// -------------------------
router.get("/me", requireAuth, async (req, res) => {
  const userId = req.user.id;

  const result = await pool.query(
    `
    SELECT
      u.id, u.username, u.tokens, u.rating, u.review_count,
      p.display_name, p.bio, p.avatar_url, p.banner_url, p.location, p.website
    FROM users u
    LEFT JOIN user_profiles p ON p.user_id = u.id
    WHERE u.id = $1
    LIMIT 1
    `,
    [userId]
  );

  const r = result.rows[0];
  res.json({
    id: Number(r.id),
    username: r.username,
    tokens: r.tokens,
    rating: r.rating,
    reviewCount: r.review_count,
    displayName: r.display_name || r.username,
    bio: r.bio || "",
    avatarUrl: r.avatar_url || "",
    bannerUrl: r.banner_url || "",
    location: r.location || "",
    website: r.website || "",
  });
});

// -------------------------
// Update my profile (+ optional username change)
// PATCH /api/profile/me
// -------------------------
router.patch("/me", requireAuth, async (req, res) => {
  const userId = req.user.id;

  // username (users table)
  const usernameRaw = req.body?.username;
  const username = usernameRaw != null ? String(usernameRaw).trim() : null;

  // profile fields (user_profiles table)
  const displayName = String(req.body?.displayName ?? "").trim().slice(0, 80);
  const bio = String(req.body?.bio ?? "").trim().slice(0, 500);
  const avatarUrl =
    req.body?.avatarUrl === undefined
      ? null
      : String(req.body?.avatarUrl ?? "").trim().slice(0, 500);
  const bannerUrl = String(req.body?.bannerUrl ?? "").trim().slice(0, 500);
  const location = String(req.body?.location ?? "").trim().slice(0, 80);
  const website = String(req.body?.website ?? "").trim().slice(0, 200);

  try {
    await pool.query("BEGIN");

    // 1) Update username if provided
    if (username !== null && username.length) {
      const normalized = username.toLowerCase();

      if (!/^[a-z0-9_]{3,20}$/.test(normalized)) {
        await pool.query("ROLLBACK");
        return res.status(400).json({
          error: "Username must be 3–20 chars (a-z, 0-9, underscore).",
        });
      }

      await pool.query(
        `UPDATE users SET username = $2 WHERE id = $1`,
        [userId, normalized]
      );
    }

    // 2) Upsert profile
    await pool.query(
      `
      INSERT INTO user_profiles (user_id, display_name, bio, avatar_url, banner_url, location, website, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7, now())
      ON CONFLICT (user_id)
      DO UPDATE SET
        display_name = EXCLUDED.display_name,
        bio = EXCLUDED.bio,
        avatar_url = COALESCE(EXCLUDED.avatar_url, user_profiles.avatar_url),
        banner_url = EXCLUDED.banner_url,
        location = EXCLUDED.location,
        website = EXCLUDED.website,
        updated_at = now()
      `,
      [userId, displayName, bio, avatarUrl, bannerUrl, location, website]
    );

    await pool.query("COMMIT");

    // return fresh profile
    const fresh = await pool.query(
      `
      SELECT
        u.id, u.username, u.tokens, u.rating, u.review_count,
        p.display_name, p.bio, p.avatar_url, p.banner_url, p.location, p.website
      FROM users u
      LEFT JOIN user_profiles p ON p.user_id = u.id
      WHERE u.id = $1
      LIMIT 1
      `,
      [userId]
    );

    const r = fresh.rows[0];
    return res.json({
      ok: true,
      profile: {
        id: Number(r.id),
        username: r.username,
        tokens: r.tokens,
        rating: r.rating,
        reviewCount: r.review_count,
        displayName: r.display_name || r.username,
        bio: r.bio || "",
        avatarUrl: r.avatar_url || "",
        bannerUrl: r.banner_url || "",
        location: r.location || "",
        website: r.website || "",
      },
    });
  } catch (e) {
    await pool.query("ROLLBACK").catch(() => {});

    if (e?.code === "23505") {
      return res.status(409).json({ error: "That username is already taken." });
    }

    console.error("Update profile error:", e);
    return res.status(500).json({ error: "Failed to update profile" });
  }
});

export default router;
