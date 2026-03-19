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

function getFrontendBase() {
  const frontendBase = String(process.env.FRONTEND_BASE_URL || "").replace(/\/$/, "");
  if (!frontendBase) {
    throw new Error("Missing FRONTEND_BASE_URL");
  }
  return frontendBase;
}

function getEmailFrom() {
  const from = process.env.EMAIL_FROM;
  if (!from) {
    throw new Error("Missing EMAIL_FROM");
  }
  return from;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function buildEmailTemplate({
  eyebrow = "DYOP",
  title,
  intro,
  actionLabel,
  actionUrl,
  footer,
  hoursText,
}) {
  const safeEyebrow = escapeHtml(eyebrow);
  const safeTitle = escapeHtml(title);
  const safeIntro = escapeHtml(intro);
  const safeActionLabel = escapeHtml(actionLabel);
  const safeFooter = escapeHtml(footer);
  const safeHoursText = escapeHtml(hoursText);
  const safeActionUrl = escapeHtml(actionUrl);

  return `
    <div style="margin:0; padding:0; background:#e7e7e7;">
      <div style="
        margin:0;
        padding:42px 20px;
        background:#e7e7e7;
        font-family:Arial, Helvetica, sans-serif;
      ">
        <div style="max-width:620px; margin:0 auto;">

          <!-- CARD -->
          <div style="
            background:#171717;
            border:1px solid #2a2a2a;
            border-radius:18px;
            padding:40px 28px 34px;
            text-align:center;
            box-shadow:0 12px 34px rgba(0,0,0,0.18);
            max-width:500px;
            margin:0 auto;
          ">

            <!-- LOGO INSIDE -->
            <div style="
              font-size:20px;
              font-weight:800;
              letter-spacing:0.05em;
              color:#ffffff;
              margin-bottom:18px;
            ">
              ${safeEyebrow}
            </div>

            <h1 style="
              margin:0 0 12px;
              font-size:24px;
              line-height:1.2;
              color:#ffffff;
              font-weight:800;
            ">
              ${safeTitle}
            </h1>

            <p style="
              margin:0 0 24px;
              font-size:15px;
              line-height:1.6;
              color:rgba(255,255,255,0.78);
            ">
              ${safeIntro}
            </p>

            <a
              href="${safeActionUrl}"
              style="
                display:inline-block;
                padding:13px 22px;
                border-radius:12px;
                background:linear-gradient(180deg, #3a3a3a, #2e2e2e);
                border:1px solid #5a5a5a;
                color:#ffffff;
                text-decoration:none;
                font-size:15px;
                font-weight:700;
              "
            >
              ${safeActionLabel}
            </a>

            <div style="
              margin-top:24px;
              padding-top:18px;
              border-top:1px solid rgba(255,255,255,0.08);
            ">
              <p style="
                margin:0 0 8px;
                font-size:12px;
                line-height:1.5;
                color:rgba(255,255,255,0.42);
              ">
                If the button doesn’t work, use this link:
              </p>

              <p style="
                margin:0;
                font-size:12px;
                line-height:1.6;
                color:rgba(255,255,255,0.52);
                word-break:break-word;
              ">
                <a href="${safeActionUrl}" style="color:#9fb0c8; text-decoration:underline;">
                  ${safeActionUrl}
                </a>
              </p>
            </div>

            <p style="
              margin:18px 0 0;
              font-size:12px;
              line-height:1.6;
              color:rgba(255,255,255,0.56);
            ">
              ${safeHoursText}
            </p>
          </div>

          <p style="
            margin:18px auto 0;
            max-width:500px;
            text-align:center;
            font-size:12px;
            line-height:1.6;
            color:#5c5c5c;
          ">
            ${safeFooter}
          </p>

        </div>
      </div>
    </div>
  `;
}

async function sendEmail({ to, subject, html, text }) {
  const from = getEmailFrom();

  const { data, error } = await resend.emails.send({
    from,
    to: [to],
    subject,
    html,
    text,
  });

  if (error) {
    throw new Error(error.message || "Failed to send email");
  }

  return data;
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

export async function sendVerificationEmail({ email, username, rawToken }) {
  const frontendBase = getFrontendBase();
  const verifyUrl = `${frontendBase}/verify-email?token=${encodeURIComponent(rawToken)}`;
  const hours = Number(process.env.EMAIL_VERIFY_HOURS || 24);

  const subject = "Verify your DYOP email";

  const html = buildEmailTemplate({
    eyebrow: "DYOP",
    title: "Verify your email",
    intro: `Hi ${username || "there"}, verify your email address to unlock uploads, comments, ratings, subscriptions, and history.`,
    actionLabel: "Verify Email",
    actionUrl: verifyUrl,
    hoursText: `This link expires in ${hours} hour${hours === 1 ? "" : "s"}.`,
    footer: "If you didn’t create an account, you can safely ignore this email.",
  });

  const text = [
    "Verify your DYOP email",
    "",
    `Hi ${username || "there"},`,
    "",
    "Verify your email address using the link below:",
    verifyUrl,
    "",
    `This link expires in ${hours} hour${hours === 1 ? "" : "s"}.`,
    "",
    "If you didn’t create an account, you can safely ignore this email.",
  ].join("\n");

  return sendEmail({
    to: email,
    subject,
    html,
    text,
  });
}

export async function sendPasswordResetEmail({ email, username, rawToken }) {
  const frontendBase = getFrontendBase();
  const resetUrl = `${frontendBase}/reset-password?token=${encodeURIComponent(rawToken)}`;
  const hours = Number(process.env.PASSWORD_RESET_HOURS || 2);

  const subject = "Reset your DYOP password";

  const html = buildEmailTemplate({
    eyebrow: "DYOP",
    title: "Reset your password",
    intro: `Hi ${username || "there"}, click below to choose a new password for your account.`,
    actionLabel: "Reset Password",
    actionUrl: resetUrl,
    hoursText: `This link expires in ${hours} hour${hours === 1 ? "" : "s"}.`,
    footer: "If you didn’t request a password reset, you can safely ignore this email.",
  });

  const text = [
    "Reset your DYOP password",
    "",
    `Hi ${username || "there"},`,
    "",
    "Use the link below to reset your password:",
    resetUrl,
    "",
    `This link expires in ${hours} hour${hours === 1 ? "" : "s"}.`,
    "",
    "If you didn’t request a password reset, you can safely ignore this email.",
  ].join("\n");

  return sendEmail({
    to: email,
    subject,
    html,
    text,
  });
}