import express from "express";
import bcrypt from "bcrypt";
import { v4 as uuid } from "uuid";
import { pool } from "./db.js";
import { getOrCreateUserMediaKey } from "./mediaKeys.js";
import {
  createEmailVerificationToken,
  invalidateUnusedVerificationTokens,
  sendVerificationEmail,
  hashToken,
  createPasswordResetToken,
  invalidateUnusedPasswordResetTokens,
  sendPasswordResetEmail,
} from "./mailer.js";

const router = express.Router();

function cookieOptions() {
  const days = Number(process.env.SESSION_DAYS || 7);
  const isProd = process.env.NODE_ENV === "production";

  return {
    httpOnly: true,
    maxAge: days * 24 * 60 * 60 * 1000,
    path: "/",
    sameSite: isProd ? "none" : "lax",
    secure: isProd,
  };
}

async function createSession(userId, res) {
  const sessionId = uuid();
  const days = Number(process.env.SESSION_DAYS || 7);

  await pool.query(
    `
    INSERT INTO sessions (id, user_id, expires_at)
    VALUES ($1, $2, now() + ($3 || ' days')::interval)
    `,
    [sessionId, userId, days]
  );

  res.cookie("session_id", sessionId, cookieOptions());
}

async function getUserBySessionId(sessionId) {
  const result = await pool.query(
    `
    SELECT
      u.id,
      u.email,
      u.username,
      u.tokens,
      u.rating,
      u.review_count,
      u.email_verified,
      u.email_verified_at,
      u.is_moderator,
      u.tier
    FROM sessions s
    JOIN users u ON u.id = s.user_id
    WHERE s.id = $1 AND s.expires_at > now()
    `,
    [sessionId]
  );

  return result.rows[0] || null;
}

router.post("/reset-password", async (req, res) => {
  const rawToken = String(req.body?.token || "").trim();
  const password = String(req.body?.password || "");

  if (!rawToken) {
    return res.status(400).json({ error: "Missing token" });
  }

  if (!password || password.length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  const tokenHash = hashToken(rawToken);

  try {
    const result = await pool.query(
      `
      SELECT id, user_id
      FROM password_reset_tokens
      WHERE token_hash = $1
        AND used_at IS NULL
        AND expires_at > now()
      LIMIT 1
      `,
      [tokenHash]
    );

    const row = result.rows[0];
    if (!row) {
      return res.status(400).json({ error: "Invalid or expired reset token" });
    }

    const passwordHash = await bcrypt.hash(password, 12);

    await pool.query("BEGIN");

    await pool.query(
      `
      UPDATE users
      SET password_hash = $2
      WHERE id = $1
      `,
      [row.user_id, passwordHash]
    );

    await pool.query(
      `
      UPDATE password_reset_tokens
      SET used_at = now()
      WHERE id = $1
      `,
      [row.id]
    );

    await pool.query(
      `
      DELETE FROM sessions
      WHERE user_id = $1
      `,
      [row.user_id]
    );

    await pool.query("COMMIT");

    return res.json({ ok: true });
  } catch (err) {
    await pool.query("ROLLBACK").catch(() => {});
    console.error("Reset password error:", err);
    return res.status(500).json({ error: "Failed to reset password" });
  }
});

router.post("/forgot-password", async (req, res) => {
  const email = String(req.body?.email || "").trim().toLowerCase();

  if (!email) {
    return res.status(400).json({ error: "Email is required" });
  }

  try {
    const result = await pool.query(
      `
      SELECT id, email, username
      FROM users
      WHERE email = $1
      LIMIT 1
      `,
      [email]
    );

    const user = result.rows[0];

    if (user) {
      await invalidateUnusedPasswordResetTokens(user.id);
      const rawToken = await createPasswordResetToken(user.id);

      await sendPasswordResetEmail({
        email: user.email,
        username: user.username,
        rawToken,
      });
    }

    return res.json({
      ok: true,
      message: "If that email exists, a password reset link has been sent.",
    });
  } catch (err) {
    console.error("Forgot password error:", err);
    return res.status(500).json({ error: "Failed to process password reset request" });
  }
});

/* register */
router.post("/register", async (req, res) => {
  const email = String(req.body?.email || "").trim().toLowerCase();
  const username = String(req.body?.username || "").trim();
  const password = String(req.body?.password || "");

  if (!email || !username || !password) {
    return res.status(400).json({ error: "Missing fields" });
  }

  const passwordHash = await bcrypt.hash(password, 12);

  try {
    const result = await pool.query(
      `
      INSERT INTO users (email, username, password_hash)
      VALUES ($1, $2, $3)
      RETURNING
        id,
        email,
        username,
        tokens,
        rating,
        review_count,
        email_verified,
        is_moderator
      `,
      [email, username, passwordHash]
    );

    const user = result.rows[0];

    await getOrCreateUserMediaKey(user.id);
    await createSession(user.id, res);

    try {
      await invalidateUnusedVerificationTokens(user.id);
      const rawToken = await createEmailVerificationToken(user.id);

      await sendVerificationEmail({
        email: user.email,
        username: user.username,
        rawToken,
      });
    } catch (mailErr) {
      console.error("Verification email send failed:", mailErr);
    }

    res.json({
      id: user.id,
      username: user.username,
      tokens: user.tokens,
      rating: user.rating,
      reviewCount: user.review_count,
      isModerator: !!user.is_moderator,
      emailVerified: !!user.email_verified,
    });
  } catch (err) {
    console.error("Register error:", err);
    res.status(400).json({ error: "User already exists" });
  }
});

/* login */
router.post("/login", async (req, res) => {
  const email = String(req.body?.email || "").trim().toLowerCase();
  const password = String(req.body?.password || "");

  const result = await pool.query(`SELECT * FROM users WHERE email = $1`, [email]);
  const user = result.rows[0];

  if (!user) return res.status(401).json({ error: "Invalid credentials" });

  const ok = await bcrypt.compare(password, user.password_hash);
  if (!ok) return res.status(401).json({ error: "Invalid credentials" });

  await getOrCreateUserMediaKey(user.id);
  await createSession(user.id, res);

  res.json({
    id: user.id,
    username: user.username,
    tokens: user.tokens,
    rating: user.rating,
    reviewCount: user.review_count,
    emailVerified: !!user.email_verified,
    isModerator: !!user.is_moderator,
  });
});

/* verify email */
router.post("/verify-email", async (req, res) => {
  const rawToken = String(req.body?.token || "").trim();
  if (!rawToken) {
    return res.status(400).json({ error: "Missing token" });
  }

  const tokenHash = hashToken(rawToken);

  try {
    const result = await pool.query(
      `
      SELECT id, user_id
      FROM email_verification_tokens
      WHERE token_hash = $1
        AND used_at IS NULL
        AND expires_at > now()
      LIMIT 1
      `,
      [tokenHash]
    );

    const row = result.rows[0];
    if (!row) {
      return res.status(400).json({ error: "Invalid or expired verification token" });
    }

    await pool.query("BEGIN");

    await pool.query(
      `
      UPDATE users
      SET email_verified = true,
          email_verified_at = now()
      WHERE id = $1
      `,
      [row.user_id]
    );

    await pool.query(
      `
      UPDATE email_verification_tokens
      SET used_at = now()
      WHERE id = $1
      `,
      [row.id]
    );

    await pool.query("COMMIT");

    return res.json({ ok: true, emailVerified: true });
  } catch (err) {
    await pool.query("ROLLBACK").catch(() => {});
    console.error("Verify email error:", err);
    return res.status(500).json({ error: "Failed to verify email" });
  }
});

/* resend verification */
router.post("/resend-verification", async (req, res) => {
  const sid = req.cookies?.session_id;
  if (!sid) return res.status(401).json({ error: "Not logged in" });

  try {
    const user = await getUserBySessionId(sid);
    if (!user) return res.status(401).json({ error: "Not logged in" });

    if (user.email_verified) {
      return res.json({ ok: true, alreadyVerified: true });
    }

    await invalidateUnusedVerificationTokens(user.id);
    const rawToken = await createEmailVerificationToken(user.id);

    await sendVerificationEmail({
      email: user.email,
      username: user.username,
      rawToken,
    });

    return res.json({ ok: true, alreadyVerified: false });
  } catch (err) {
    console.error("Resend verification error:", err);
    return res.status(500).json({ error: "Failed to resend verification email" });
  }
});

/* logout */
router.post("/logout", async (req, res) => {
  const sid = req.cookies?.session_id;

  if (sid) {
    await pool.query(`DELETE FROM sessions WHERE id = $1`, [sid]);
  }

  res.clearCookie("session_id", cookieOptions());
  res.json({ ok: true });
});

/* current user */
router.get("/me", async (req, res) => {
  const sid = req.cookies?.session_id;
  if (!sid) return res.status(401).json(null);

  const result = await pool.query(
    `
    SELECT
      u.id,
      u.username,
      u.tokens,
      u.rating,
      u.tier,
      u.review_count,
      u.email_verified,
      u.is_moderator,
      u.tier
    FROM sessions s
    JOIN users u ON u.id = s.user_id
    WHERE s.id = $1 AND s.expires_at > now()
    `,
    [sid]
  );

  if (!result.rows[0]) return res.status(401).json(null);

  await getOrCreateUserMediaKey(result.rows[0].id);

  res.json({
    id: result.rows[0].id,
    username: result.rows[0].username,
    tokens: result.rows[0].tokens,
    rating: result.rows[0].rating,
    reviewCount: result.rows[0].review_count,
    isModerator: !!result.rows[0].is_moderator,
    tier: result.rows[0].review_count,
    emailVerified: !!result.rows[0].email_verified,
  });
});

export default router;