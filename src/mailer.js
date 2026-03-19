import crypto from "crypto";
import { Resend } from "resend";
import { pool } from "./db.js";

const resend = new Resend(process.env.RESEND_API_KEY);

export function hashToken(token) {
  return crypto.createHash("sha256").update(token).digest("hex");
}

export function makeVerificationToken() {
  return crypto.randomBytes(32).toString("hex");
}

export async function createEmailVerificationToken(userId) {
  const rawToken = makeVerificationToken();
  const tokenHash = hashToken(rawToken);
  const hours = Number(process.env.EMAIL_VERIFY_HOURS || 24);

  await pool.query(
    `
    INSERT INTO email_verification_tokens (user_id, token_hash, expires_at)
    VALUES ($1, $2, now() + ($3 || ' hours')::interval)
    `,
    [userId, tokenHash, hours]
  );

  return rawToken;
}

export async function invalidateUnusedVerificationTokens(userId) {
  await pool.query(
    `
    UPDATE email_verification_tokens
    SET used_at = now()
    WHERE user_id = $1
      AND used_at IS NULL
      AND expires_at > now()
    `,
    [userId]
  );
}

export async function createPasswordResetToken(userId) {
  const rawToken = makeVerificationToken();
  const tokenHash = hashToken(rawToken);
  const hours = Number(process.env.PASSWORD_RESET_HOURS || 2);

  await pool.query(
    `
    INSERT INTO password_reset_tokens (user_id, token_hash, expires_at)
    VALUES ($1, $2, now() + ($3 || ' hours')::interval)
    `,
    [userId, tokenHash, hours]
  );

  return rawToken;
}

export async function invalidateUnusedPasswordResetTokens(userId) {
  await pool.query(
    `
    UPDATE password_reset_tokens
    SET used_at = now()
    WHERE user_id = $1
      AND used_at IS NULL
      AND expires_at > now()
    `,
    [userId]
  );
}

export async function sendPasswordResetEmail({ email, username, rawToken }) {
  const frontendBase = String(process.env.FRONTEND_BASE_URL || "").replace(/\/$/, "");
  if (!frontendBase) throw new Error("Missing FRONTEND_BASE_URL");

  const from = process.env.EMAIL_FROM;
  if (!from) throw new Error("Missing EMAIL_FROM");

  const resetUrl = `${frontendBase}/reset-password?token=${encodeURIComponent(rawToken)}`;

  const subject = "Reset your DYOPorn password";

  const html = `
    <div style="font-family: Arial, sans-serif; line-height:1.5; color:#111;">
      <h2>Reset your password</h2>
      <p>Hi ${username || "there"},</p>
      <p>Click the button below to reset your password.</p>
      <p>
        <a
          href="${resetUrl}"
          style="display:inline-block; padding:12px 18px; background:#111; color:#fff; text-decoration:none; border-radius:8px;"
        >
          Reset password
        </a>
      </p>
      <p>Or use this link:</p>
      <p><a href="${resetUrl}">${resetUrl}</a></p>
      <p>This link expires in ${Number(process.env.PASSWORD_RESET_HOURS || 2)} hours.</p>
    </div>
  `;

  const text = [
    "Reset your DYOPorn password",
    "",
    `Hi ${username || "there"},`,
    "",
    "Use the link below to reset your password:",
    resetUrl,
    "",
    `This link expires in ${Number(process.env.PASSWORD_RESET_HOURS || 2)} hours.`,
  ].join("\n");

  const { data, error } = await resend.emails.send({
    from,
    to: [email],
    subject,
    html,
    text,
  });

  if (error) {
    throw new Error(error.message || "Failed to send password reset email");
  }

  return data;
}

export async function sendVerificationEmail({ email, username, rawToken }) {
  const frontendBase = String(process.env.FRONTEND_BASE_URL || "").replace(/\/$/, "");
  if (!frontendBase) {
    throw new Error("Missing FRONTEND_BASE_URL");
  }

  const from = process.env.EMAIL_FROM;
  if (!from) {
    throw new Error("Missing EMAIL_FROM");
  }

  const verifyUrl = `${frontendBase}/verify-email?token=${encodeURIComponent(rawToken)}`;

  const subject = "Verify your DYOPorn email";

  const html = `
    <div style="font-family: Arial, sans-serif; line-height:1.5; color:#111;">
      <h2>Verify your email</h2>
      <p>Hi ${username || "there"},</p>
      <p>Welcome to DYOPorn. Please verify your email address by clicking the button below.</p>
      <p>
        <a
          href="${verifyUrl}"
          style="display:inline-block; padding:12px 18px; background:#111; color:#fff; text-decoration:none; border-radius:8px;"
        >
          Verify email
        </a>
      </p>
      <p>Or use this link:</p>
      <p><a href="${verifyUrl}">${verifyUrl}</a></p>
      <p>This link expires in ${Number(process.env.EMAIL_VERIFY_HOURS || 24)} hours.</p>
    </div>
  `;

  const text = [
    "Verify your DYOPorn email",
    "",
    `Hi ${username || "there"},`,
    "",
    "Welcome to DYOP. Please verify your email address using the link below:",
    verifyUrl,
    "",
    `This link expires in ${Number(process.env.EMAIL_VERIFY_HOURS || 24)} hours.`,
  ].join("\n");

  const { data, error } = await resend.emails.send({
    from,
    to: [email],
    subject,
    html,
    text,
  });

  if (error) {
    throw new Error(error.message || "Failed to send verification email");
  }

  return data;
}