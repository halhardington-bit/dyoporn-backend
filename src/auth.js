import express from "express";
import bcrypt from "bcrypt";
import crypto from "crypto";
import passport from "passport";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";
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

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function calculateAge(dateString) {
  if (!dateString) return null;

  const birthDate = new Date(dateString);
  if (Number.isNaN(birthDate.getTime())) return null;

  const today = new Date();
  let age = today.getFullYear() - birthDate.getFullYear();
  const monthDiff = today.getMonth() - birthDate.getMonth();

  if (
    monthDiff < 0 ||
    (monthDiff === 0 && today.getDate() < birthDate.getDate())
  ) {
    age -= 1;
  }

  return age;
}

function normalizeCountry(value) {
  const country = String(value || "").trim();
  return country || null;
}

function makeRandomPassword() {
  return crypto.randomBytes(32).toString("hex");
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
      u.tier,
      u.date_of_birth,
      u.country
    FROM sessions s
    JOIN users u ON u.id = s.user_id
    WHERE s.id = $1 AND s.expires_at > now()
    `,
    [sessionId]
  );

  return result.rows[0] || null;
}

passport.use(
  new GoogleStrategy(
    {
      clientID: process.env.GOOGLE_CLIENT_ID,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET,
      callbackURL:
        process.env.NODE_ENV === "production"
          ? "https://api.dyop.ai/auth/google/callback"
          : "http://localhost:3001/auth/google/callback",
    },
    async (accessToken, refreshToken, profile, done) => {
      try {
        const provider = "google";
        const providerUserId = String(profile.id);
        const email =
          profile.emails?.[0]?.value?.trim().toLowerCase() || null;
        const displayName =
          profile.displayName?.trim() ||
          profile.name?.givenName?.trim() ||
          "user";

        const identityResult = await pool.query(
          `
          SELECT u.id, u.username
          FROM user_identities ui
          JOIN users u ON u.id = ui.user_id
          WHERE ui.provider = $1 AND ui.provider_user_id = $2
          LIMIT 1
          `,
          [provider, providerUserId]
        );

        if (identityResult.rows[0]) {
          return done(null, { id: identityResult.rows[0].id });
        }

        // No auto-linking by email in this version.
        const baseUsername = displayName
          .toLowerCase()
          .replace(/[^a-z0-9_]/g, "_")
          .replace(/_+/g, "_")
          .replace(/^_+|_+$/g, "")
          .slice(0, 24) || "user";

        let username = baseUsername;
        let suffix = 1;

        while (true) {
          const existing = await pool.query(
            `SELECT id FROM users WHERE username = $1 LIMIT 1`,
            [username]
          );
          if (!existing.rows[0]) break;
          suffix += 1;
          username = `${baseUsername}_${suffix}`.slice(0, 32);
        }

        const randomPassword = makeRandomPassword();
        const passwordHash = await bcrypt.hash(randomPassword, 12);

        await pool.query("BEGIN");

        const userInsert = await pool.query(
          `
          INSERT INTO users (
            email,
            username,
            password_hash,
            email_verified
          )
          VALUES ($1, $2, $3, $4)
          RETURNING id
          `,
          [email, username, passwordHash, true]
        );

        const userId = userInsert.rows[0].id;

        await pool.query(
          `
          INSERT INTO user_identities (
            user_id,
            provider,
            provider_user_id,
            email_at_provider
          )
          VALUES ($1, $2, $3, $4)
          `,
          [userId, provider, providerUserId, email]
        );

        await pool.query("COMMIT");

        return done(null, { id: userId });
      } catch (err) {
        await pool.query("ROLLBACK").catch(() => {});
        return done(err);
      }
    }
  )
);

// Passport session serialize/deserialize are required by passport,
// but we are still using your own cookie session system for the app.
passport.serializeUser((user, done) => done(null, user.id));
passport.deserializeUser((id, done) => done(null, { id }));

router.post("/reset-password", async (req, res) => {
  const rawToken = String(req.body?.token || "").trim();
  const password = String(req.body?.password || "");

  if (!rawToken) {
    return res.status(400).json({ error: "Missing token" });
  }

  if (!password || password.length < 8) {
    return res
      .status(400)
      .json({ error: "Password must be at least 8 characters" });
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
    return res
      .status(500)
      .json({ error: "Failed to process password reset request" });
  }
});

router.post("/register", async (req, res) => {
  const email = String(req.body?.email || "").trim().toLowerCase();
  const username = String(req.body?.username || "").trim();
  const password = String(req.body?.password || "");
  const dateOfBirth = String(req.body?.dateOfBirth || "").trim();
  const country = normalizeCountry(req.body?.country);

  if (!email || !username || !password || !dateOfBirth) {
    return res.status(400).json({
      error: "Email, username, password, and date of birth are required",
    });
  }

  if (!isValidEmail(email)) {
    return res.status(400).json({ error: "Please enter a valid email address" });
  }

  if (username.length < 3) {
    return res
      .status(400)
      .json({ error: "Username must be at least 3 characters" });
  }

  if (username.length > 32) {
    return res
      .status(400)
      .json({ error: "Username must be 32 characters or fewer" });
  }

  if (!/^[a-zA-Z0-9_]+$/.test(username)) {
    return res.status(400).json({
      error: "Username can only contain letters, numbers, and underscores",
    });
  }

  if (password.length < 8) {
    return res
      .status(400)
      .json({ error: "Password must be at least 8 characters" });
  }

  const age = calculateAge(dateOfBirth);

  if (age == null) {
    return res.status(400).json({ error: "Please enter a valid date of birth" });
  }

  if (age < 18) {
    return res
      .status(400)
      .json({ error: "You must be at least 18 years old to create an account" });
  }

  const passwordHash = await bcrypt.hash(password, 12);

  try {
    const result = await pool.query(
      `
      INSERT INTO users (
        email,
        username,
        password_hash,
        date_of_birth,
        country
      )
      VALUES ($1, $2, $3, $4, $5)
      RETURNING
        id,
        email,
        username,
        tokens,
        rating,
        review_count,
        email_verified,
        date_of_birth,
        country,
        is_moderator,
        tier
      `,
      [email, username, passwordHash, dateOfBirth, country]
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

    return res.json({
      id: user.id,
      username: user.username,
      tokens: user.tokens,
      rating: user.rating,
      reviewCount: user.review_count,
      isModerator: !!user.is_moderator,
      emailVerified: !!user.email_verified,
      tier: user.tier,
      dateOfBirth: user.date_of_birth,
      country: user.country,
    });
  } catch (err) {
    console.error("Register error:", err);

    if (err?.code === "23505") {
      if (String(err.constraint || "").includes("email")) {
        return res.status(400).json({ error: "An account with that email already exists" });
      }
      if (String(err.constraint || "").includes("username")) {
        return res.status(400).json({ error: "That username is already taken" });
      }
      return res.status(400).json({ error: "User already exists" });
    }

    if (err?.code === "23514") {
      return res.status(400).json({ error: "Account does not meet age requirements" });
    }

    return res.status(500).json({ error: "Failed to create account" });
  }
});

router.post("/login", async (req, res) => {
  const email = String(req.body?.email || "").trim().toLowerCase();
  const password = String(req.body?.password || "");

  try {
    const result = await pool.query(
      `
      SELECT
        id,
        email,
        username,
        password_hash,
        tokens,
        rating,
        review_count,
        email_verified,
        is_moderator,
        tier,
        date_of_birth,
        country
      FROM users
      WHERE email = $1
      LIMIT 1
      `,
      [email]
    );

    const user = result.rows[0];

    if (!user) {
      return res.status(401).json({ error: "Invalid credentials" });
    }

    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok) {
      return res.status(401).json({ error: "Invalid credentials" });
    }

    await getOrCreateUserMediaKey(user.id);
    await createSession(user.id, res);

    return res.json({
      id: user.id,
      username: user.username,
      tokens: user.tokens,
      rating: user.rating,
      reviewCount: user.review_count,
      emailVerified: !!user.email_verified,
      isModerator: !!user.is_moderator,
      tier: user.tier,
      dateOfBirth: user.date_of_birth,
      country: user.country,
    });
  } catch (err) {
    console.error("Login error:", err);
    return res.status(500).json({ error: "Failed to log in" });
  }
});

router.get(
  "/google",
  passport.authenticate("google", {
    scope: ["profile", "email"],
    session: false,
  })
);

router.get(
  "/google/callback",
  passport.authenticate("google", {
    failureRedirect:
      process.env.NODE_ENV === "production"
        ? `${process.env.FRONTEND_BASE_URL}/?auth=google_failed`
        : "http://localhost:5173/?auth=google_failed",
    session: false,
  }),
  async (req, res) => {
    try {
      const userId = req.user.id;

      await getOrCreateUserMediaKey(userId);
      await createSession(userId, res);

      const redirectTo =
        process.env.NODE_ENV === "production"
          ? `${process.env.FRONTEND_BASE_URL}/?auth=google_success`
          : "http://localhost:5173/watch?auth=google_success";

      return res.redirect(redirectTo);
    } catch (err) {
      console.error("Google callback session error:", err);

      const redirectTo =
        process.env.NODE_ENV === "production"
          ? `${process.env.FRONTEND_BASE_URL}/?auth=google_failed`
          : "http://localhost:5173/?auth=google_failed";

      return res.redirect(redirectTo);
    }
  }
);

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
      return res
        .status(400)
        .json({ error: "Invalid or expired verification token" });
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
    return res
      .status(500)
      .json({ error: "Failed to resend verification email" });
  }
});

router.post("/logout", async (req, res) => {
  const sid = req.cookies?.session_id;

  if (sid) {
    await pool.query(`DELETE FROM sessions WHERE id = $1`, [sid]);
  }

  res.clearCookie("session_id", cookieOptions());
  res.json({ ok: true });
});

router.get("/me", async (req, res) => {
  const sid = req.cookies?.session_id;
  if (!sid) return res.status(401).json(null);

  try {
    const result = await pool.query(
      `
      SELECT
        u.id,
        u.username,
        u.tokens,
        u.rating,
        u.review_count,
        u.email_verified,
        u.is_moderator,
        u.tier,
        u.date_of_birth,
        u.country
      FROM sessions s
      JOIN users u ON u.id = s.user_id
      WHERE s.id = $1 AND s.expires_at > now()
      `,
      [sid]
    );

    if (!result.rows[0]) return res.status(401).json(null);

    await getOrCreateUserMediaKey(result.rows[0].id);

    return res.json({
      id: result.rows[0].id,
      username: result.rows[0].username,
      tokens: result.rows[0].tokens,
      rating: result.rows[0].rating,
      reviewCount: result.rows[0].review_count,
      isModerator: !!result.rows[0].is_moderator,
      tier: result.rows[0].tier,
      emailVerified: !!result.rows[0].email_verified,
      dateOfBirth: result.rows[0].date_of_birth,
      country: result.rows[0].country,
    });
  } catch (err) {
    console.error("/me error:", err);
    return res.status(500).json({ error: "Failed to fetch current user" });
  }
});

export default router;